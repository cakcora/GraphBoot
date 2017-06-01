

import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 1/12/2017.
  */
object Embeddedness {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("embeddedness")
      .master("local[4]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext

    val dataset: String = "gowalla"
    println("data set is: " + dataset)
    var graph: Graph[Int, Int] = DataLoader.loadGraph(sc, dataset, Map())
    if (List("facebook", "dblp", "test", "kite", "gowalla").contains(dataset))
      graph = GraphCleaning.cleanGraph(sc, graph)
    else if (List("enron", "wiki", "epinions").contains(dataset))
      graph = GraphCleaning.undirectedGraph(graph, 1)
    else {
      println(dataset + " network is not an existing dataset. ")
    }
    println(graph.numEdges + " cleaned edges among " + graph.numVertices + " vertices")
    val degreeMap: Map[VertexId, Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either).collect().toMap
    var embedAll = ListBuffer.empty[Double]
    for (vertexInfo <- degreeMap) {
      val vertex = vertexInfo._1
      var embedV = 0.0;
      val neighbors = vertexInfo._2

      for (neighbor <- neighbors) {
        val intersect = degreeMap(neighbor).intersect(neighbors).toSet.size
        val union = degreeMap(neighbor).union(neighbors).toSet.size - 2 //-2 excludes vertex and neighbor itself
        if (union != 0) {
          embedV += intersect.toDouble / union
        }
      }
      if (!neighbors.isEmpty) {
        embedV /= neighbors.length
      }
      embedAll.append(embedV)
    }
    val wr = new FileWriter(dataset + "Embeddedness.txt")
    wr.append(embedAll.mkString("\r\n")).close()

  }


}
