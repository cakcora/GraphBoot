import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 1/23/2017.
  */
object Tester {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()

    val sc = spark.sparkContext
    val map = scala.collection.mutable.HashMap.empty[Int, Int].withDefaultValue(0)

    for (a <- List(1, 2, 1, 3, 10, 9, 11)) {
      map(a) += 1
  }
    val text = map.toSeq.sorted.map(e => (e._1 + ":" + e._2)).mkString(" ")
    println(text)
  }


}
