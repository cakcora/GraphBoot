import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
  */
object ExperimentDriverValueNetworks {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val dataset = "epinions-value"
    val values = List(5044, 3069, 46214) //6854272->3069//526227072->5044, 462395008->46214
    for (value <- values) {
      println(s"data set is:$dataset $value")
      val fw: FileWriter = new FileWriter("exp" + dataset + value + ".txt");
      val header = "method\twave\tseed\tlmsiAll\tlmsiDistinct\tmean\tmedGraphDeg\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tmuProxy\tl2\tlmin\tlmax\n"
      fw.write(header)
      val wave = 2


      val degreeMap: Map[Int, Int] = DataLoader.loadData(sc, value)
      println(degreeMap.size + " users with the topic found")
      val graph: Graph[Int, Int] = GraphCleaning.cleanGraph(sc, degreeMap, DataLoader.loadGraph(sc, dataset, Map()))
      println(graph.numEdges + " directed edges among " + graph.numVertices + " vertices")


      println(graph.numEdges + " cleaned edges among " + graph.numVertices + " vertices")
      val degree = degreeMap.values.sum / (1.0 * degreeMap.keySet.size)
      val valueFile: FileWriter = new FileWriter("value" + dataset + value + ".txt");
      degreeMap.foreach(e => valueFile.append(e._1 + "\t" + e._2 + "\r\n"))
      valueFile.close()
      print(degree + " average degrees")
      for (iteration <- 1 to 50) {
        println(" iter " + iteration)
        val maxSeed = 100
        val allSeeds: RDD[(VertexId, Int)] = Common.chooseKnownSeeds(sc, degreeMap, maxSeed)

        for (seedCount <- List(1, 5, 10, 20, 50, 100)) {
          val expOptions: Map[String, Int] = Map(("bootCount", 1000), ("wave", wave))
          val seedSet: Array[(VertexId, Int)] = allSeeds.take(seedCount)

          val muProxy: Double = Common.proxyMu(allSeeds.takeSample(true, seedCount).map(e => e._1.toInt), degreeMap)
          val txtPar = GraphBoot.compute(sc, graph, degreeMap, seedSet, expOptions, "parSpark")
          printResults(fw, wave, muProxy, seedCount, expOptions, txtPar, "parSpark")

          fw.flush()
        }

      }
      fw.close()
    }
  }

  def printResults(fw: FileWriter, wave: Int, muProxy: Double, seed: Int, expOptions: Map[String, Int], txt: Map[String, AnyVal], method: String): Unit = {
    fw.write(method + "\t" + wave + "\t" + seed + "\t" + txt("lmsiAll") + "\t" + txt("lmsiDistinct") + "\t" + txt("mean") + "\t" + txt("medGraphDeg") + "\t" + txt("avgGraphDeg") + "\t" + txt("varianceOfBootStrapDegrees") + "\t" + txt("l1") + "\t" + muProxy + "\t" + txt("l2") + "\t" + txt("lmin") + "\t" + txt("lmax") + "\n")
  }


}
