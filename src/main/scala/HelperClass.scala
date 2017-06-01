import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by cxa123230 on 1/12/2017.
  */
object HelperClass {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[6]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext

    val dataset: String = "kite"
    val graph: Graph[Int, Int] = GraphCleaning.cleanGraph(sc, DataLoader.loadGraph(sc, dataset, Map()))
    if (true) {
      print(dataset, graph)
    }
    println(graph.numEdges + " " + graph.numVertices)


  }


  def print(dataset: String, graph: Graph[Int, Int]): Unit = {
    val degreeMap = mutable.Map.empty[Int, Int].withDefaultValue(0)
    for (x <- graph.degrees.collect()) {
      degreeMap(x._2) += 1
    }
    val sum = degreeMap.map(_._2).sum.toDouble
    val fw: FileWriter = new FileWriter(dataset + "Dist.txt");
    for (x <- degreeMap) {
      fw.append(x._1 + "\t" + x._2 + "\t" + x._2 / sum + "\n")
    }
    fw.close()
  }
}
