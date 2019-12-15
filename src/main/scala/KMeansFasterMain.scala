import java.io.{FileOutputStream, PrintStream}
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession

object KMeansFasterMain {

  def main(args: Array[String]): Unit = {

    System.setOut(new PrintStream(new FileOutputStream("log.out.txt")))

    // important from spark 2.0 to run the job on the cluster
    val sparkSession = SparkSession.builder()
      .appName(s"KMeans_Scala_Klouvi_Riva${scala.util.Random.nextInt()}")
      //.master("local")
      .getOrCreate()

    //Iris data path is passed a first argument
    val path = args(0) //"hdfs:///user/user159/iris.data.txt"

    val maxPartitions = 1
    val nbClusters = 3
    val maxSteps = 100
    val seed = 42

    val t0 = System.nanoTime()

    val engine: KMeansFasterHandler = new KMeansFasterHandler(sparkSession.sparkContext, path, maxPartitions)
    engine.initialize()

    val centroids = KMeansHelper.track("Getting the initial centroids", {
      val c = engine.getCentroids(nbClusters, seed).persist()
      KMeansHelper.logRDD("centroids", c)
      c
    })

    val clustering = KMeansHelper.track("Building cluster", {
      engine.build(centroids, maxSteps)
    })

    val duration =  TimeUnit.SECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS)
    val outputPath = s"${args(0)}/../kmeans_output"
    val metricsPath = s"${args(0)}/../kmeans_metrics"

    val metrics = sparkSession.sparkContext.parallelize(KMeansHelper.LogBuffer)

    KMeansHelper.logRDD("clusters", clustering._1)
    KMeansHelper.log(s"output path: $outputPath")
    KMeansHelper.log(s"metrics path: $metricsPath")
    KMeansHelper.log(s"error: ${clustering._2}")
    KMeansHelper.log(s"number_of_steps: ${clustering._3}")
    KMeansHelper.log(s"duration: $duration s")
    clustering._1.sortBy(x => x._2._1._1).coalesce(1).saveAsTextFile(outputPath)
    metrics.coalesce(1).saveAsTextFile(metricsPath)

    sparkSession.close()
  }
}
