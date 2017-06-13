import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by cxa123230 on 1/12/2017.
  */
object DistributionHandler {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[6]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext

    val dataset: String = "livejournal"
    val graph: Graph[Int, Int] = GraphCleaning.undirectedGraph(DataLoader.loadGraph(sc, dataset, Map()), 1)
    if (true) {
      print(dataset, graph)
    }
    println(graph.numEdges + " " + graph.numVertices)


  }


  def print(dataset: String, graph: Graph[Int, Int]): Unit = {
    val degreeMap = mutable.Map.empty[Int, Int].withDefaultValue(0)
    for (x <- graph.degrees.values.collect()) {
      degreeMap(x) += 1
    }
    val sum = degreeMap.values.sum.toDouble
    val fw: FileWriter = new FileWriter(dataset + "Dist.txt");
    for (x <- degreeMap) {
      fw.append(x._1 + "\t" + x._2 + "\t" + x._2 / sum + "\n")
    }
    fw.close()
  }
}
