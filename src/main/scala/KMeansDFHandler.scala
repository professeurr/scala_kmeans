import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.math.sqrt


class KMeansDFHandler(spark: SparkSession, path: String, partitions: Int = 1) extends Serializable {

  private val sc = spark.sparkContext
  //val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)


  protected var data: RDD[(Long, Array[Double])] = _
  protected var labels: RDD[(Long, String)] = _
  protected var dataFrame: DataFrame = _

  def schema: StructType = StructType(Seq(
    StructField(name = "id", dataType = LongType, nullable = false)
    , StructField(name = "data_cords", dataType = ArrayType(DoubleType), nullable = false)
    , StructField(name = "label", dataType = StringType, nullable = false)
  ))

  def initialize(): Unit = {

    val lines = KMeansHelper.track(s"Reading data from $path", {
      spark.sparkContext.textFile(path = path)
    })

    dataFrame = KMeansHelper.track("Loading data and creating data frame", {
      val d = lines.map(x => x.split(','))
        .zipWithIndex() //zipWithIndex allows us to give a specific index to each point
        .map(x => Row(x._2, x._1.take(4).map(KMeansHelper.toDouble), x._1(4))) //swap the index and the array positions
      spark.createDataFrame(d, schema) //.orderBy(rand())
    })
    dataFrame.show()
  }

  def getCentroids(nbClusters: Int): Dataset[Row] = {
    // Select initial centroids
    dataFrame.orderBy(randn(42))
      .limit(nbClusters)
      .createOrReplaceTempView("centroids")
    val centroids = spark.sql("select id as cid, data_cords as centroid_cords from centroids ")
    centroids
  }

  def build(centroids: Dataset[Row], maxStep: Int): Unit = {
    var clusteringDone = false
    val myData = dataFrame.as("data")
    val myCentroids = centroids.as("centroids")

    while (!clusteringDone) {
      val joined = dataFrame.crossJoin(centroids)
      joined.show()

      val dist = joined.withColumn("dist", rand()) //KMeansHelper.udfComputeDistance(col("data_cords"), col("centroid_cords"))
      dist.show()

      val assignment = dist.groupBy("id")
        .agg(min("dist").alias("dist"))
      assignment.show(false)

      val ass = dist.join(assignment, dist("id") <=> assignment("id") && dist("dist") <=> assignment("dist"))
      ass.show()

      clusteringDone = true
    }
  }

  def build2(centroids: RDD[(Long, Array[Double])], maxSteps: Int): (RDD[(Long, ((Long, Double), Array[String]))], Double, Long) = {
    var clusteringDone = false
    var number_of_steps = 1
    var error: Double = 0.0
    var prev_assignment: RDD[(Long, ((Long, Double), Array[Double]))] = null
    var assignment: RDD[(Long, ((Long, Double), Array[Double]))] = null
    var currentCentroids = centroids

    //A broadcast value is sent to and saved by each executor for further use
    //instead of being sent to each executor when needed.
    val nb_elem = sc.broadcast(data.count())

    while (!clusteringDone) {
      KMeansHelper.logTitle(s"Step $number_of_steps")

      //Assign points to clusters
      val joined = data.cartesian(currentCentroids) //((0,Array(5.1, 3.5, 1.4, 0.2)),(1,Array(4.8, 3.1, 1.6, 0.2)))
      KMeansHelper.logRDD("joined", joined)
      KMeansHelper.log(s"joined partitions: ${joined.getNumPartitions}")

      //We compute the distance between the points and each cluster
      // Append also the data point to the distance list to avoid the later join()
      // that way we reduce significantly the number of shuffles (data transfer across nodes)
      val dist = joined.map(x => (x._1._1, ((x._2._1, KMeansHelper.computeDistance(x._1._2, x._2._2)), x._1._2)))
      //(0,((1,0.5385164807134504),Array(5.1, 3.5, 1.4, 0.2)))
      KMeansHelper.logRDD("dist", dist)
      KMeansHelper.log(s"dist partitions: ${dist.getNumPartitions}")

      //assignment will be our      return value: It contains the datapoint      ,
      //the id of the closest cluster and the distance of the point to the centroid
      assignment = dist.reduceByKey((x, y) => if (x._1._2 < y._1._2) x else y) //(19,((2,0.6855654600401041),Array(5.1, 3.8, 1.5, 0.3)))
      KMeansHelper.logRDD("assignment", assignment)
      KMeansHelper.log(s"assignment partitions: ${assignment.getNumPartitions}")

      //Compute the new centroid of each cluster
      // Prepare the data points for the counting operation
      val clusters = assignment.map(z => (z._2._1._1, (1, z._2._2, z._2._1._2))) //(2,(1,Array(5.1, 3.8, 1.5, 0.3),0.6855654600401041))
      KMeansHelper.logRDD("clusters", clusters)
      // Count the number of data points of each cluster and sum up the data points coordinates
      val count = clusters.reduceByKey((x, y) => (x._1 + y._1, KMeansHelper.sumList(x._2, y._2), x._3 + y._3))
      // Compute the new centroids of the clusters
      currentCentroids = count.map(x => (x._1, KMeansHelper.meanList(x._2._2, x._2._1))) //(0,Array(6.301030927835052, 2.8865979381443303, 4.958762886597938, 1.6958762886597938))
      KMeansHelper.logRDD(s"currentCentroids", currentCentroids)
      KMeansHelper.log(s"currentCentroids partitions: ${currentCentroids.getNumPartitions}")

      //Is the clustering over ?
      //Let's see how many points have switched clusters
      val switch = if (prev_assignment != null) assignment.join(prev_assignment).filter(x => x._2._1._1 != x._2._2._1).count() else nb_elem.value
      KMeansHelper.log(s"switch: $switch")

      if (switch == 0 || number_of_steps > maxSteps) {
        clusteringDone = true
        //Use count rdd to compute to reduce because it contains less data
        error = sqrt(count.map(x => x._2._3).reduce((x, y) => x + y)) / nb_elem.value
        //error = sqrt(assignment.map(x => x._2._1._2).reduce((x, y) => x + y)) / nb_elem.value
        //KMeansHelper.log(s"error1: $error")
      }
      else {
        prev_assignment = assignment
        number_of_steps += 1
      }
    }

    //Assign label to each data point
    val cluster = KMeansHelper.track("Setting up the cluster labels", {
      assignment.join(labels).map(x => (x._1, (x._2._1._1, x._2._1._2.map(s => s.toString) :+ x._2._2)))
    })

    (cluster, error, number_of_steps)
  }
}
