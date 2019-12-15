import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object KMeansDFMain {

  //KMeansHelper.Log = true
  //KMeansHelper.Debug = false

  // important from spark 2.0 to run the job on the cluster
  private val sparkSession = SparkSession.builder()
    .appName(s"KMeansScala${scala.util.Random.nextInt()}")
    //.master("local")
    .getOrCreate()

  private val conf = new SparkConf().setAppName("KMeansScala").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val path = "hdfs://localhost:9090/bigdata/project/kmeans/iris.data.txt"
  val maxPartitions = 1
  val nbClusters = 3
  val maxSteps = 100

  val kmDf = new KMeansDFHandler(sparkSession, path)
  kmDf.initialize()

  private val centroids = kmDf.getCentroids(nbClusters)
  centroids.show(nbClusters)
  kmDf.build(centroids, maxSteps)

}
