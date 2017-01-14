import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 1/12/2017.
  */
object Tester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val df = proxyMu(Array(1, 5, 3), Map((1, 1), (2, 2), (5, 100)))
    println(df)

  }

  def proxyMu(seeds: Array[(Int)], degreeMap: Map[Int, Int]): Any = {

    val degreeList: Vector[Double] = degreeMap.filter(e => seeds.contains(e._1)).map(e => e._2.toDouble).toVector
    return breeze.stats.meanAndVariance(degreeList)
  }
}
