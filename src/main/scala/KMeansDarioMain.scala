import org.apache.spark.{SparkConf, SparkContext}


object KMeansDarioMain {

  private val conf = new SparkConf().setAppName("KMeansScala").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val path = "hdfs://localhost:9090/bigdata/project/kmeans/iris.data.txt"
  val maxPartitions = 1
  val nbClusters = 3
  val maxSteps = 100
  val seed = 42

  val engine: KMeansDarioHandler = new KMeansDarioHandler(sc, path)
  engine.initialize()

  private val centroids = KMeansHelper.track("Getting the initial centroids", {
    val c = engine.getCentroids(nbClusters, seed).persist()
    KMeansHelper.logRDD("centroids", c)
    c
  })

  KMeansHelper.track("Building cluster", {
    KMeansHelper.logTitle(s"Building cluster with ${engine.getClass.getName}")
    val clustering = engine.build(centroids, maxSteps)
    //clustering._1.coalesce(1).saveAsTextFile(s"hdfs://localhost:9090/bigdata/project/kmeans/output${t1}")
    KMeansHelper.logTitle(s"Result for ${engine.getClass.getName}")
    //clustering._1.sortBy(x => x._2._1._1).collect().foreach(x => KMeansHelper.log(stringOf(x))) //.saveAsTextFile("src/main/data/result.txt")
    KMeansHelper.logRDD("clusters", clustering._1)
    KMeansHelper.log(s"error: ${clustering._2}")
    KMeansHelper.log(s"number_of_steps: ${clustering._3}")
  })
}
