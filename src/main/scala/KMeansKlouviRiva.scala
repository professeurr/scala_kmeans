import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.sqrt
import scala.runtime.ScalaRunTime.stringOf


object KMeansKlouviRiva {

  /** ************************
   * Global variables definition
   */
  // the coordinates holder
  protected var data: RDD[(Long, Array[Double])] = _
  //the labels holder
  protected var labels: RDD[(Long, String)] = _

  // if DisplayRDD=true, the RDD are collected and display
  var DisplayRDD = false

  // Initialize spark session. Important from spark 2.0 to run the job on the cluster
  val sparkSession: SparkSession = SparkSession.builder()
    .appName(s"KMeans_Scala_Klouvi_Riva${scala.util.Random.nextInt()}")
    .master("local")
    .getOrCreate()

  // Get the sparkcontext from the session
  val sc: SparkContext = sparkSession.sparkContext

  /** *****************************
   * Read csv data from the given path
   *
   * @param path: the data path
   */
  def readData(path: String): Unit = {
    val lines = track(s"Reading data from $path", {
      sc.textFile(path = path)
    })

    val inputs = track("Preparing data (split, conversion, index)", {
      val d = lines.map(x => {
        val y = x.split(',')
        (y.dropRight(1).map(toDouble), y.last)
      }).zipWithIndex() //zipWithIndex allows us to give a specific index to each point

      log(s"Number of data: ${d.count}")
      log(s"data partitions: ${d.getNumPartitions}")
      d
    })

    // Split the des data points into 2 RDD. The labels assignment will be performed at the end of the computation
    // 1) <data> contains the coordinates mapped with their id of the data points
    //2) <labels> contains the data points labels mapped with their id
    data = inputs.map(x => (x._2, x._1._1)).persist() //(0, Array(5.1, 3.5, 1.4, 0.2))
    labels = inputs.map(x => (x._2, x._1._2)) //(0, Iris-setosa)

    logRDD("data", data)
    logRDD("labels", labels)
  }


  /** ***************************
   * This function reads the data from the given @path and build the cluster
   *
   * @param path: path
   * @param maxSteps: number of steps
   * @param partitions: used to control the RDD partition over the cluster
   * @param seed: the random generator seed
   * @return
   */
  def buildCluster(path: String, maxSteps: Int, partitions: Int, seed: Int): (RDD[(Long, ((Long, Double), (Double, Double, Double, Double, String)))], Double, Int) = {
    var clusteringDone = false
    var number_of_steps = 1
    var error: Double = 0.0
    var prev_assignment: RDD[(Long, ((Long, Double), Array[Double]))] = null
    var assignment: RDD[(Long, ((Long, Double), Array[Double]))] = null

    // read data from the given path
    readData(path)

    // Compute the size of clusters
    val clusters = labels.map(x => (x._2, x._1)).reduceByKey((_, _) => 1).count()
    log(s"Number of clusters: $clusters")

    // Select initial centroids
    var currentCentroids = sc.parallelize(data.takeSample(withReplacement = false, clusters.toInt, seed = seed))
      .zipWithIndex()
      .map(x => (x._2, x._1._2))
    logRDD("centroids", currentCentroids)

    //A broadcast value is sent to and saved by each executor for further use
    //instead of being sent to each executor when needed.
    val nb_elem = sc.broadcast(data.count())

    // Keep looping until the cluster is successfully built
    while (!clusteringDone) {
      logTitle(s"Step $number_of_steps")

      //Assign points to centroids
      var joined = data.cartesian(currentCentroids) //((0,Array(5.1, 3.5, 1.4, 0.2)),(1,Array(4.8, 3.1, 1.6, 0.2)))
      logRDD("joined", joined)
      log(s"joined partitions: ${joined.getNumPartitions}")

      //Reduce number of partitions
      joined = joined.coalesce(numPartitions = partitions)
      log(s"joined partitions after coalesce(): ${joined.getNumPartitions}")

      // We compute the distance between the points and each cluster
      // Append also the data point to the distance list to avoid the later join()
      // that way we reduce significantly the number of shuffles (data transfer across nodes)
      val dist = joined.map(x => (x._1._1, ((x._2._1, computeDistance(x._1._2, x._2._2)), x._1._2))) //(0, ((1,0.5385164807134504), Array(5.1, 3.5, 1.4, 0.2)))

      logRDD("dist", dist)
      log(s"dist partitions: ${dist.getNumPartitions}")

      //assignment will be our return value: It contains the datapoint      ,
      //the id of the closest cluster and the distance of the point to the centroid
      assignment = dist.reduceByKey((x, y) => if (x._1._2 < y._1._2) x else y) //(19, ((2,0.6855654600401041), Array(5.1, 3.8, 1.5, 0.3)))
      logRDD("assignment", assignment)
      log(s"assignment partitions: ${assignment.getNumPartitions}")

      //Compute the new centroid of each cluster
      // Prepare the data points for the counting and summation operations
      val clusters = assignment.map(z => (z._2._1._1, (1, z._2._2, z._2._1._2))) //(2,(1, Array(5.1, 3.8, 1.5, 0.3),0.6855654600401041))
      logRDD("clusters", clusters)
      // Count the number of data points of each cluster and sum up the data points coordinates
      val count = clusters.reduceByKey((x, y) => (x._1 + y._1, sumList(x._2, y._2), x._3 + y._3))
      // Compute the new centroids of the clusters
      currentCentroids = count.map(x => (x._1, meanList(x._2._2, x._2._1))) //(0, Array(6.301030927835052, 2.8865979381443303, 4.958762886597938, 1.6958762886597938))
      logRDD(s"currentCentroids", currentCentroids)
      log(s"currentCentroids partitions: ${currentCentroids.getNumPartitions}")

      //Is the clustering over ?
      //Let's see how many points have switched clusters
      val switch = if (prev_assignment != null) assignment.join(prev_assignment).filter(x => x._2._1._1 != x._2._2._1).count() else nb_elem.value
      log(s"switch: $switch")

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
    val cluster = track("Setting up the cluster labels", {
      assignment.join(labels).map(x => (x._1, (x._2._1._1, (x._2._1._2(0), x._2._1._2(1), x._2._1._2(2), x._2._1._2(3), x._2._2))))
    })

    (cluster, error, number_of_steps)
  }

  /** *****************************
   * The main function to run the program
   *
   * @param args: list of the arguments provided to the application
   */
  def main(args: Array[String]): Unit = {

    //Iris data path is passed a first argument
    val path = args(0) //"hdfs:///user/user159/iris.data.txt"
    val maxPartitions = 1 // Runtime.getRuntime().availableProcessors() - 1
    val maxSteps = 100
    val seed = 42
    val t0 = System.nanoTime()

    // Build the cluster
    val clustering = track("Building cluster", {
      buildCluster(path, maxSteps, maxPartitions, seed)
    })

    // Format the output data points and metrics
    val duration = TimeUnit.SECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS)
    val outputPath = s"${args(0)}/../kmeans_scala_cluster"
    val metricsPath = s"${args(0)}/../kmeans_scala_metrics"

    logRDD("clusters", clustering._1)
    log(s"clusters path: $outputPath")
    log(s"metrics path: $metricsPath")
    log(s"error: ${clustering._2}")
    logTitle("Metrics")
    log(s"number_of_steps: ${clustering._3}")
    log(s"duration: $duration s")

    val metrics = sc.parallelize(LogBuffer)
    // write out the cluster
    clustering._1.sortBy(x => x._2._1._1).coalesce(1).saveAsTextFile(outputPath)
    // write out the log + metrics
    metrics.coalesce(1).saveAsTextFile(metricsPath)

    // Close spark session, release resources
    sparkSession.close()
  }


  /** ***************************
   * Some utility functions
   */
  def computeDistance(x: Array[Double], y: Array[Double]): Double = {
    sqrt((for (z <- x.zip(y)) yield (z._1 - z._2) * (z._1 - z._2)).sum)
  }

  def sumList(x: Array[Double], y: Array[Double]): Array[Double] = {
    for (z <- x.zip(y)) yield z._1 + z._2
  }

  def meanList(x: Array[Double], n: Int): Array[Double] = {
    for (z <- x) yield z / n
  }

  def closestCluster(dist_list: Array[(Long, Double)]): (Long, Double) = {
    var z = dist_list.head
    for (elem <- dist_list)
      if (elem._2 < z._2)
        z = elem
    z
  }

  def toDouble(x: Any): Double = {
    x.toString.toDouble
  }

  var LogBuffer: Array[String] = Array()

  def logRDD[T](label: String, data: RDD[T]): Unit = {
    if (DisplayRDD)
      log(s"$label: ${stringOf(data.collect())}")
    log(s"dist partitions: ${data.getNumPartitions}")
  }

  def log(x: String): Unit = {
    LogBuffer = LogBuffer :+ x
  }

  def logTitle(x: String): Unit = {
    log(s"============== $x ============== ")
  }

  def track[R](label: String, block: => R): R = {
    log(label + "...")
    val t0 = System.nanoTime()
    val result = block
    log(s"Done (${TimeUnit.SECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS)} s)")
    result
  }
}
