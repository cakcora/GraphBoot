import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 1/23/2017.
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
    var graph: Graph[Int, Int] = DataLoader.load(sc, "wiki", Map())
    println(graph.numEdges + " directed edges among " + graph.numVertices + " vertices")
    //    graph = GraphCleaning.cleanGraph(sc, graph)
    graph = GraphCleaning.undirectedGraph(graph, 1)
    println(graph.numEdges + " directed edges among " + graph.numVertices + " vertices")
  }

}
