import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
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
    val arr = Array((1L, 2L), (1L, 3L), (2L, 3L), (2L, 4L), (3L, 4L), (4L, 5L), (4L, 6L), (5L, 6L))
    val graph = Graph.fromEdgeTuples(sc.makeRDD(arr), defaultValue = 1)
    val l1 = LMSI.singleSeed(graph.edges, 1, 2)
    val l2 = LMSI.singleSeed(graph.edges, 6, 2)

    println(l1.union(l2))
    val l3 = LMSI.multipleSeeds(graph, List(1, 6), 2)
    println(l3)


  }


}
