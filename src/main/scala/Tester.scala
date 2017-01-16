import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
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
    val arr = Array((1L, 5L), (1L, 6L), (5L, 7L), (6L, 7L), (7L, 9L), (6L, 8L), (8L, 10L), (10L, 3L), (9L, 4L), (9L, 11L), (11L, 12L), (2L, 4L), (2L, 3L))
    val graph = Graph.fromEdgeTuples(sc.makeRDD(arr), defaultValue = 1)
    val ls = LMSI.parallelLMSI(graph, sc.makeRDD(Array((1L, 0), (2L, 0))), 2)
    println(ls.sorted)


  }


}
