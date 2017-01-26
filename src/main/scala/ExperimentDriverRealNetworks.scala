import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
  */
object ExperimentDriverRealNetworks {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val networkName = "wiki"
    println("data set is: " + networkName)
    val fw: FileWriter = new FileWriter("expWaved" + networkName + ".2txt");
    val header = "method\twave\tseed\tlmsiAll\tlmsiDistinct\tmean\tmedGraphDeg\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tmuProxy\tl2\tlmin\tlmax\n"
    fw.write(header);
    val wave = 2


    var graph: Graph[Int, Int] = DataLoader.load(sc, networkName, Map())
    println(graph.numEdges + " directed edges among " + graph.numVertices + " vertices")
    if (List("facebook", "dblp").contains(networkName))
      graph = GraphCleaning.cleanGraph(sc, graph)
    else if (List("enron", "wiki", "epinions").contains(networkName))
      graph = GraphCleaning.undirectedGraph(graph, 1)
    else {
      throw new IllegalArgumentException(networkName + " network is not available")
    }
    val degreeMap: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    println(graph.numEdges + " cleaned edges among " + graph.numVertices + " vertices")
    for (iteration <- 1 to 50) {
      println(" iter " + iteration)
      val maxSeed = 100
      val allSeeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, maxSeed)
      val seedCount = 10
      for (wave <- List(0, 1, 2, 3, 4, 5, 6, 7, 8)) {
        val expOptions: Map[String, Int] = Map(("bootCount", 1000), ("wave", wave))
        println("we have seed " + seedCount)
        val seedSet: Array[(VertexId, Int)] = allSeeds.take(seedCount)

        val muProxy: Double = Common.proxyMu(allSeeds.takeSample(true, seedCount).map(e => e._1.toInt), degreeMap)
        val txtPar = GraphBoot.compute(sc, graph, degreeMap, seedSet, expOptions, "parSpark")
        printResults(fw, wave, muProxy, seedCount, expOptions, txtPar, "parSpark")
        //        val txtSeq = GraphBoot.compute(sc, graph, degreeMap, seedSet, expOptions, "seq")
        //        printResults(fw, wave, muProxy, seedCount, expOptions, txtSeq, "seq")

        fw.flush()
      }

    }
    fw.close()
  }

  def printResults(fw: FileWriter, wave: Int, muProxy: Double, seed: Int, expOptions: Map[String, Int], txt: Map[String, AnyVal], method: String): Unit = {
    fw.write(method + "\t" + wave + "\t" + seed + "\t" + txt("lmsiAll") + "\t" + txt("lmsiDistinct") + "\t" + txt("mean") + "\t" + txt("medGraphDeg") + "\t" + txt("avgGraphDeg") + "\t" + txt("varianceOfBootStrapDegrees") + "\t" + txt("l1") + "\t" + muProxy + "\t" + txt("l2") + "\t" + txt("lmin") + "\t" + txt("lmax") + "\n")
  }


}
