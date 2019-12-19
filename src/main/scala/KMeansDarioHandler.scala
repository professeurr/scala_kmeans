import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math.sqrt


class KMeansDarioHandler(sc: SparkContext, path: String)  extends Serializable {

  private var data: RDD[(Long, Array[String])] = _

  def initialize(): Unit = {

    val lines = KMeansHelper.track(s"Reading data from $path", {
      sc.textFile(path = path)
    })

    data = KMeansHelper.track("Preparing data (split, conversion, index)", {
      val d = lines.map(x => x.split(','))
        .zipWithIndex() //zipWithIndex allows us to give a specific index to each point
        .map(x => (x._2, x._1)) //swap the index and the array positions
      KMeansHelper.log(s"Number of data: ${d.count}")
      KMeansHelper.log(s"data partitions: ${d.getNumPartitions}")
      KMeansHelper.logRDD("data", d) //(0,Array(5.1, 3.5, 1.4, 0.2, Iris-setosa))
      d
    })
  }

  def getCentroids(nbClusters: Int, seed: Long): RDD[(Long, Array[Double])] = {
    // Select initial centroids
    sc.parallelize(data.takeSample(withReplacement = false, nbClusters, seed = seed))
      .zipWithIndex()
      .map(x => (x._2, x._1._2.take(4).map(KMeansHelper.toDouble)))
  }

  def build(centroids: RDD[(Long, Array[Double])], maxSteps: Int): (RDD[(Long, ((Long, Double), Array[String]))], Double, Long) = {
    var clusteringDone = false
    var number_of_steps = 1
    var switch: Long = 150
    var error: Double = 0.0
    var prev_assignment: RDD[(Long, (Long, Double))] = null
    var assignment: RDD[(Long, ((Long, Double), Array[String]))] = null
    var centroidsCluster = centroids

    //A broadcast value is sent to and saved by each executor for further use
    //instead of being sent to each executor when needed.
    val nb_elem = sc.broadcast(data.count())

    while (!clusteringDone) {

      KMeansHelper.logTitle(s"Step $number_of_steps")

      //Assign points to clusters
      //print('centroids: {}    '.format(centroids.getNumPartitions())    )
      val joined = data.cartesian(centroidsCluster)
      KMeansHelper.logRDD("joined", joined) //( (1, Array(4.9, 3.0, 1.4, 0.2, Iris-setosa)),(0,Array(5.4, 3.9, 1.7, 0.4)))
      KMeansHelper.log(s"joined partitions: ${joined.getNumPartitions}")

      //We compute the distance between the points and each cluster
      val dist = joined.map(x => (x._1._1, (x._2._1,
        KMeansHelper.computeDistance(x._1._2.take(4).map(KMeansHelper.toDouble),
          x._2._2.map(KMeansHelper.toDouble)))))
      KMeansHelper.logRDD("dist", dist) //(0,(2,0.22360679774997896)), (1,(0,1.0908712114635715))
      KMeansHelper.log(s"dist partitions: ${dist.getNumPartitions}")

      val dist_list = dist.groupByKey().mapValues(x => x.toArray)
      KMeansHelper.logRDD("dist_list", dist_list) //(19,Array((0,5.478138369920935), (1,4.342810150121693), (2,2.7294688127912363)))
      KMeansHelper.log(s"dist_list partitions: ${dist_list.getNumPartitions}")

      //We keep only the closest cluster to each point.
      val min_dist = dist_list.mapValues(x => KMeansHelper.closestCluster(x))
      KMeansHelper.logRDD("min_dist", min_dist) //((19,(2,2.7294688127912363))
      KMeansHelper.log(s"min_dist partitions: ${min_dist.getNumPartitions}")

      //assignment will be our      return value: It contains the datapoint      ,
      //the id of the closest cluster and the distance of the point to the centroid
      assignment = min_dist.join(data)
      KMeansHelper.logRDD("assignment", assignment) //(19,((2,2.7294688127912363),Array(5.1, 3.8, 1.5, 0.3, Iris-setosa)))
      KMeansHelper.log(s"assignment partitions: ${assignment.getNumPartitions}")

      //
      //Compute the new centroid of each cluster
      //
      val clusters = assignment.map(x => (x._2._1._1, x._2._2.take(4).map(KMeansHelper.toDouble)))
      KMeansHelper.logRDD("clusters", clusters) //(2, [5.1, 3.5, 1.4, 0.2] )
      val count = clusters.map(x => (x._1, 1)).reduceByKey((x, y) => x + y)
      val somme = clusters.reduceByKey((x, y) => KMeansHelper.sumList(x, y))
      centroidsCluster = somme.join(count).map(x => (x._1, KMeansHelper.meanList(x._2._1, x._2._2)))
      KMeansHelper.log(s"centroidsCluster: ${centroidsCluster.getNumPartitions}")
      KMeansHelper.log(s"centroidsCluster partitions: ${centroidsCluster.getNumPartitions}")

      //Is the clustering over ?
      //Let's see how many points have switched clusters
      if (number_of_steps > 1) {
        switch = prev_assignment.join(min_dist).filter(x => x._2._1._1 != x._2._2._1).count
        KMeansHelper.log(s"switch: $switch")
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
}
