import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math.sqrt


class KMeansFasterHandler(sc: SparkContext, path: String, partitions: Int = 1) extends Serializable {

  protected var data: RDD[(Long, Array[Double])] = _
  protected var labels: RDD[(Long, String)] = _

  def initialize(): Unit = {
    val lines = KMeansHelper.track(s"Reading data from $path", {
      sc.textFile(path = path)
    })

    val inputs = KMeansHelper.track("Preparing data (split, conversion, index)", {
      val d = lines.map(x => {
        val y = x.split(',')
        (y.dropRight(1).map(KMeansHelper.toDouble), y.last)
      }).zipWithIndex() //zipWithIndex allows us to give a specific index to each point

      KMeansHelper.log(s"Number of data: ${d.count}")
      KMeansHelper.log(s"data partitions: ${d.getNumPartitions}")
      d
    })

    // Split the des data points into 2 RDD. The labels assignment will be performed at the end of the computation
    // 1) <data> contains the coordinates mapped with their id of the data points
    //2) <labels> contains the data points labels mapped with their id
    data = inputs.map(x => (x._2, x._1._1)).persist() //(0,Array(5.1, 3.5, 1.4, 0.2))
    labels = inputs.map(x => (x._2, x._1._2)) //(0,Iris-setosa)

    KMeansHelper.logRDD("data", data)
    KMeansHelper.logRDD("labels", labels)
  }

  def build(maxSteps: Int, seed: Int): (RDD[(Long, ((Long, Double), (Double, Double, Double, Double, String)))], Double, Int) = {
    var clusteringDone = false
    var number_of_steps = 1
    var error: Double = 0.0
    var prev_assignment: RDD[(Long, ((Long, Double), Array[Double]))] = null
    var assignment: RDD[(Long, ((Long, Double), Array[Double]))] = null

    // Compute the size of clusters
    val clusters = labels.map(x => (x._2, x._1)).reduceByKey((_, _) => 1).count()
    KMeansHelper.log(s"Number of clusters: $clusters")

    // Select initial centroids
    var currentCentroids = sc.parallelize(data.takeSample(withReplacement = false, clusters.toInt, seed = seed))
      .zipWithIndex()
      .map(x => (x._2, x._1._2))
    KMeansHelper.logRDD("centroids", currentCentroids)

    //A broadcast value is sent to and saved by each executor for further use
    //instead of being sent to each executor when needed.
    val nb_elem = sc.broadcast(data.count())

    while (!clusteringDone) {
      KMeansHelper.logTitle(s"Step $number_of_steps")

      //Assign points to centroids
      var joined = data.cartesian(currentCentroids) //((0,Array(5.1, 3.5, 1.4, 0.2)),(1,Array(4.8, 3.1, 1.6, 0.2)))
      KMeansHelper.logRDD("joined", joined)
      KMeansHelper.log(s"joined partitions: ${joined.getNumPartitions}")

      //Reduce number of partitions
      joined = joined.coalesce(numPartitions = partitions)
      KMeansHelper.log(s"joined partitions after coalesce(): ${joined.getNumPartitions}")

      //We compute the distance between the points and each cluster
      // Append also the data point to the distance list to avoid the later join()
      // that way we reduce significantly the number of shuffles (data transfer across nodes)
      val dist = joined.map(x => (x._1._1, ((x._2._1, KMeansHelper.computeDistance(x._1._2, x._2._2)), x._1._2))) //(0,((1,0.5385164807134504),Array(5.1, 3.5, 1.4, 0.2)))

      KMeansHelper.logRDD("dist", dist)
      KMeansHelper.log(s"dist partitions: ${dist.getNumPartitions}")

      //assignment will be our return value: It contains the datapoint      ,
      //the id of the closest cluster and the distance of the point to the centroid
      assignment = dist.reduceByKey((x, y) => if (x._1._2 < y._1._2) x else y) //(19,((2,0.6855654600401041),Array(5.1, 3.8, 1.5, 0.3)))
      KMeansHelper.logRDD("assignment", assignment)
      KMeansHelper.log(s"assignment partitions: ${assignment.getNumPartitions}")

      //Compute the new centroid of each cluster
      // Prepare the data point for the counting and summation operations
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
      }
      else {
        prev_assignment = assignment
        number_of_steps += 1
      }
    }

    //Assign label to each data point
    val cluster = KMeansHelper.track("Setting up the cluster labels", {
      assignment.join(labels).map(x => (x._1, (x._2._1._1, (x._2._1._2(0), x._2._1._2(1), x._2._1._2(2), x._2._1._2(3), x._2._2))))
    })

    (cluster, error, number_of_steps)
  }
}
