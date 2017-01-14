import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
  */
object ExperimentDriverDBLP {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val fw: FileWriter = new FileWriter("expDBLP.txt");
    val header = "method\twave\tseed\tnumVertices\tnumEdges\tmean\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tmuProxy\tl2\tlmin\tlmax\n"
    fw.write(header);
    val wave = 2
    val expOptions: Map[String, Int] = Map(("bootCount", 1000), ("wave", wave))
    val grOptions: Map[String, AnyVal] = Map()

    for (iteration <- 1 to 50) {
      println(" iter " + iteration)
      var graph: Graph[Int, Int] = SyntheticData.synthGraphGenerator(sc, "dblp", grOptions)
      graph = GraphCleaning.cleanGraph(sc, graph.mapVertices((a, b) => a.toInt))
      val degreeMap: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
      val maxSeed = 512
      val allSeeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, maxSeed)
      for (seedCount <- List(1, 1, 1, 1, 2, 4, 8, 16, 32, 64, 128, 256, maxSeed)) {
        println("we have seed " + seedCount)
        val seedSet: Array[(VertexId, Int)] = allSeeds.take(seedCount)

        val muProxy: Double = Common.proxyMu(seedSet.map(e => e._1.toInt), degreeMap)
        println("muproxy is " + muProxy)
        val txtPar = GraphBoot.compute(sc, graph, degreeMap, seedSet, expOptions, "parSpark")
        printResults(fw, wave, muProxy, seedCount, expOptions, txtPar, "parSpark")
        val txtSeq = GraphBoot.compute(sc, graph, degreeMap, seedSet, expOptions, "seq")
        printResults(fw, wave, muProxy, seedCount, expOptions, txtSeq, "seq")

        fw.flush()
      }

    }
    fw.close()
  }

  def printResults(fw: FileWriter, wave: Int, muProxy: Double, seed: Int, expOptions: Map[String, Int], txtPar: Map[String, AnyVal], method: String): Unit = {
    fw.write(method + "\t" + wave + "\t" + seed + "\t" + txtPar("vertices") + "\t" + txtPar("edges") + "\t" + txtPar("mean") + "\t" + txtPar("avgGraphDeg") + "\t" + txtPar("varianceOfBootStrapDegrees") + "\t" + txtPar("l1") + "\t" + muProxy + "\t" + txtPar("l2") + "\t" + txtPar("lmin") + "\t" + txtPar("lmax") + "\n")
  }


}
