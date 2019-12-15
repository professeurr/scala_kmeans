import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.math.sqrt
import scala.runtime.ScalaRunTime.stringOf

object KMeansHelper extends Serializable {

  var Debug = false

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

  def logRDD[T](label: String, data: RDD[T]): Unit = {
    if (Debug)
      println(s"$label: ${stringOf(data.collect())}")
  }

  var Log = false

  def log(x: String): Unit = {
    if (Log)
      println(x)
  }

  def logTitle(x: String): Unit = {
    if (Log)
      println(s"============== $x ============== ")
  }

  def track[R](label: String, block: => R): R = {
    log(label + "...")
    val t0 = System.nanoTime()
    val result = block
    log(s"Done (${TimeUnit.SECONDS.convert(System.nanoTime() - t0, TimeUnit.NANOSECONDS)} s)")
    result
  }

  def testFunc(): Unit = {
    val x: Array[Double] = Array(1.0, 2, 3)
    val y: Array[Double] = Array(2.0, 3, 4)
    val z: Array[Long] = Array(2, 3, 4)

    println(KMeansHelper.computeDistance(x, y))
    println(stringOf(KMeansHelper.sumList(x, y)))
    println(stringOf(KMeansHelper.meanList(x, 3)))
    val c = KMeansHelper.closestCluster(z.zip(y))
    println(c)
  }

  def udfComputeDistance: UserDefinedFunction = udf((a: Seq[Double], b: Seq[Double]) => {
    computeDistance(a.toArray, b.toArray)
  })

}
