import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

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
    val arr = Array((1L, 3L), (1L, 4L), (3L, 2L), (4L, 6L), (6L, 2L))
    val graph = Graph.fromEdgeTuples(sc.makeRDD(arr), defaultValue = 1)
    val l1 = LMSI.singleSeed(graph.edges, 1, 2)
    val l2 = LMSI.singleSeed(graph.edges, 2, 2)

    println(l1.union(l2))
    val l3 = LMSI.parallelLMSI(graph, sc.makeRDD(Array((1L, 0), (2L, 0))), 2)
    println(l3)

    val g = SyntheticData.synthGraphGenerator(sc, "dblp", Map(("mu", 2.0), ("sigma", 2.0), ("vertices", 100)))

    val h = g.collectNeighbors(EdgeDirection.Either).collect().map(e => e._2).map(e => e.length)
    var m = mutable.HashMap[Int, Int]().withDefaultValue(0)
    for (e <- h) {
      m(e) += 1
    }
    for (e <- m)
      print(e._1 + "\t" + e._2 + "\n")
  }


}
