import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.sqrt
import scala.runtime.ScalaRunTime.stringOf


object KMeansDario {

  /** ************************
   * Global variables definition
   */

  // the coordinates holder
  protected var data: RDD[(Long, Array[String])] = _

  // if DisplayRDD=true, the RDD are collected and display
  var DisplayRDD = false

  // Initialize spark session. Important from spark 2.0 to run the job on the cluster
  val sparkSession: SparkSession = SparkSession.builder()
    .appName(s"KMeans_Scala_Klouvi_Riva${scala.util.Random.nextInt()}")
    //.master("local")
    .getOrCreate()

  // Get the sparkContext from the session
  val sc: SparkContext = sparkSession.sparkContext

  /** *****************************
   * Read csv data from the given path
   *
   * @param path : the data path
   */
  def readData(path: String): Unit = {

    val lines = track(s"Reading data from $path", {
      sc.textFile(path = path)
    })

    data = track("Preparing data (split, conversion, index)", {
      val d = lines.map(x => x.split(','))
        .zipWithIndex() //zipWithIndex allows us to give a specific index to each point
        .map(x => (x._2, x._1)) //swap the index and the array positions
      log(s"Number of data: ${d.count}")
      log(s"data partitions: ${d.getNumPartitions}")
      logRDD("data", d) //(0,Array(5.1, 3.5, 1.4, 0.2, Iris-setosa))
      d
    })

    logRDD("data", data)
  }


  def buildCluster(path: String, nbClusters: Int, maxSteps: Int, partitions: Int, seed: Int): (RDD[(Long, ((Long, Double), Array[String]))], Double, Long) = {
    var clusteringDone = false
    var number_of_steps = 1
    var switch: Long = 150
    var error: Double = 0.0
    var prev_assignment: RDD[(Long, (Long, Double))] = null
    var assignment: RDD[(Long, ((Long, Double), Array[String]))] = null

    // read data from the given path
    readData(path)

    // Select initial centroids
    var centroidsCluster = sc.parallelize(data.takeSample(withReplacement = false, nbClusters, seed = seed))
      .zipWithIndex()
      .map(x => (x._2, x._1._2.take(4).map(toDouble)))

    //A broadcast value is sent to and saved by each executor for further use
    //instead of being sent to each executor when needed.
    val nb_elem = sc.broadcast(data.count())

    while (!clusteringDone) {

      logTitle(s"Step $number_of_steps")

      //Assign points to clusters
      //print('centroids: {}    '.format(centroids.getNumPartitions())    )
      val joined = data.cartesian(centroidsCluster)
      logRDD("joined", joined) //( (1, Array(4.9, 3.0, 1.4, 0.2, Iris-setosa)),(0,Array(5.4, 3.9, 1.7, 0.4)))
      log(s"joined partitions: ${joined.getNumPartitions}")

      //We compute the distance between the points and each cluster
      val dist = joined.map(x => (x._1._1, (x._2._1,
        computeDistance(x._1._2.take(4).map(toDouble),
          x._2._2.map(toDouble)))))
      logRDD("dist", dist) //(0,(2,0.22360679774997896)), (1,(0,1.0908712114635715))
      log(s"dist partitions: ${dist.getNumPartitions}")

      val dist_list = dist.groupByKey().mapValues(x => x.toArray)
      logRDD("dist_list", dist_list) //(19,Array((0,5.478138369920935), (1,4.342810150121693), (2,2.7294688127912363)))
      log(s"dist_list partitions: ${dist_list.getNumPartitions}")

      //We keep only the closest cluster to each point.
      val min_dist = dist_list.mapValues(x => closestCluster(x))
      logRDD("min_dist", min_dist) //((19,(2,2.7294688127912363))
      log(s"min_dist partitions: ${min_dist.getNumPartitions}")

      //assignment will be our return value: tt contains the datapoints,
      //the id of the closest cluster and the distance of the point to the centroid
      assignment = min_dist.join(data)
      logRDD("assignment", assignment) //(19,((2,2.7294688127912363),Array(5.1, 3.8, 1.5, 0.3, Iris-setosa)))
      log(s"assignment partitions: ${assignment.getNumPartitions}")

      //
      //Compute the new centroid of each cluster
      //
      val clusters = assignment.map(x => (x._2._1._1, x._2._2.take(4).map(toDouble)))
      logRDD("clusters", clusters) //(2, [5.1, 3.5, 1.4, 0.2] )
      val count = clusters.map(x => (x._1, 1)).reduceByKey((x, y) => x + y)
      val somme = clusters.reduceByKey((x, y) => sumList(x, y))
      centroidsCluster = somme.join(count).map(x => (x._1, meanList(x._2._1, x._2._2)))
      log(s"centroidsCluster: ${centroidsCluster.getNumPartitions}")
      log(s"centroidsCluster partitions: ${centroidsCluster.getNumPartitions}")

      //Is the clustering over ?
      //Let's see how many points have switched clusters
      if (number_of_steps > 1) {
        switch = prev_assignment.join(min_dist).filter(x => x._2._1._1 != x._2._2._1).count
        log(s"switch: $switch")
      }

      if (switch == 0 || number_of_steps == maxSteps) {
        clusteringDone = true
        error = sqrt(min_dist.map(x => x._2._2).reduce((x, y) => x + y)) / nb_elem.value
      }
      else {
        prev_assignment = min_dist
        number_of_steps += 1
      }
    }

    (assignment, error, number_of_steps)
  }

  /** *****************************
   * The main function to run the program
   *
   * @param args : list of the arguments provided to the application
   */
  def mainx(args: Array[String]): Unit = {

    //Iris data path is passed a first argument
    val path = args(0) //"hdfs:///user/user159/iris.data.txt"
    val maxPartitions = 1 // Runtime.getRuntime().availableProcessors() - 1
    val maxSteps = 100
    val nbCluster = 3
    val seed = 42
    val t0 = System.nanoTime()

    // Build the cluster
    val clustering = track("Building cluster", {
      buildCluster(path, nbCluster, maxSteps, maxPartitions, seed)
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
