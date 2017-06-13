import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
  */
object ExperimentDriverRealNetworks {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[6]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val dataSets = List("dblp", "facebook", "enron", "gowalla", "kite", "epinions", "livejournal", "wiki-talk")
    val exDataSet = List("livejournal")
    for (dataset <- exDataSet) {
      println("data set is: " + dataset)
      val fw: FileWriter = new FileWriter("exp" + dataset + ".txt");
      val header = "method\twave\tseed\tlmsiAll\tlmsiDistinct\tmean\tmedGraphDeg\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tmuProxy\tl2\tlmin\tlmax\n"
      fw.write(header);
      val wave = 2


      var graph: Graph[Int, Int] = DataLoader.loadGraph(sc, dataset, Map())
      println(graph.numEdges + " directed edges among " + graph.numVertices + " vertices")
      if (List("facebook", "dblp", "gowalla", "kite").contains(dataset))
        graph = GraphCleaning.cleanGraph(sc, graph)
      else if (List("enron", "wiki", "epinions", "livejournal").contains(dataset))
        graph = GraphCleaning.undirectedGraph(graph, 1)
      else {
        //graph = GraphCleaning.cleanGraph(sc, graph)
        throw new IllegalArgumentException(dataset + " network is not available")
      }
      println(graph.numEdges + " cleaned edges among " + graph.numVertices + " vertices")
      val degreeMap: Map[Int, Int] = graph.degrees.map(e => (e._1.toInt, e._2)).collect().toMap
      println(degreeMap.size + " nodes among " + degreeMap.values.sum + " degrees")
      val degree = degreeMap.map(e => e._2).sum / (1.0 * graph.numVertices)
      val valueFile: FileWriter = new FileWriter("value" + dataset + ".txt");
      degreeMap.foreach(e => valueFile.append(e._1 + "\t" + e._2 + "\r\n"))
      valueFile.close
      print(degree + " average degrees")
      for (iteration <- 1 to 50) {
        println(" iter " + iteration)
        val maxSeed = 100
        val allSeeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, maxSeed)
        val seedCount = 20
        //for (seedCount <- List(1, 5, 10, 20, 50, maxSeed))
        for (wave <- List(0, 1, 2, 3, 4, 5, 6, 7, 8)) {
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
