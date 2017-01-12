import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
  */
object LMSITester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val arr = Array((1L, 3L), (1L, 4L), (3L, 2L), (4L, 6L), (6L, 2L), (3l, 6L), (3L, 7L), (7L, 8L))
    val graph = Graph.fromEdgeTuples(sc.makeRDD(arr), defaultValue = 1)


    val l1 = LMSI.sequentialLMSI(sc, graph, Array((1L, 0), (2L, 0)), 0)
    val l3 = LMSI.parallelLMSI(graph, sc.makeRDD(Array((1L, 0), (2L, 0))), 0)
    assert(l1.diff(List(1L, 2L)).length == 0)
    assert(l1.diff(l3).length == 0)


  }
}