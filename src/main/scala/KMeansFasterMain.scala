import org.apache.spark.sql.SparkSession

object KMeansFasterMain {

  KMeansHelper.Log = true
  KMeansHelper.Debug = false

  def main(args: Array[String]): Unit = {

    //System.setOut(new PrintStream(new FileOutputStream("log.out.txt")))

    // important from spark 2.0 to run the job on the cluster
    val sparkSession = SparkSession.builder()
      .appName(s"KMeansScala${scala.util.Random.nextInt()}")
      //.master("local")
      .getOrCreate()

    //Iris data path is passed a first argument
    val path = args(0) //"hdfs:///user/user159/iris.data.txt"

    val maxPartitions = 1
    val nbClusters = 3
    val maxSteps = 100
    val seed = 42

    val engine: KMeansFasterHandler = new KMeansFasterHandler(sparkSession.sparkContext, path, maxPartitions)
    engine.initialize()

    val centroids = KMeansHelper.track("Getting the initial centroids", {
      val c = engine.getCentroids(nbClusters, seed).persist()
      KMeansHelper.logRDD("centroids", c)
      c
    })

    val clustering = KMeansHelper.track("Building cluster", {
      KMeansHelper.logTitle(s"Building cluster with ${engine.getClass.getName}")
      engine.build(centroids, maxSteps)
    })

    val outputPath = s"${args(0)}/../kmeans_output"
    KMeansHelper.track(s"Saving the clustering result into $outputPath", {
      clustering._1.sortBy(x => x._2._1._1).coalesce(1).saveAsTextFile(outputPath)
    })

    KMeansHelper.logRDD("clusters", clustering._1)
    KMeansHelper.log(s"error: ${clustering._2}")
    KMeansHelper.log(s"number_of_steps: ${clustering._3}")

    /*sparkSession.sparkContext.textFile(path = "log.out.txt", 1)
      .saveAsTextFile(s"${args(1)}/log")
*/
    sparkSession.close()
  }
}
