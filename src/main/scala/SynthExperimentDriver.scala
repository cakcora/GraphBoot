import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, Graph, _}
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/30/2016.
  */
object SynthExperimentDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[12]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger().setLevel(Level.ERROR)
    val fw = new FileWriter("wave.txt", true);
    val header = "wave\tmu\tsigma\tvertices\tseedCount\tbootCount\tpatchCount\tpx\tnumVertices\tnumEdges\tmean\tavgGraphDeg\tmeanOfintervalLengths\tvarianceOfpatchDegrees\tC\n"
    fw.write(header);
    for (v <- 1 to 100) {
      println(v + " runs.")
      run(sc, fw)
    }
    fw.close()
    sc.stop()
  }

  def run(sc: SparkContext, fw: FileWriter) = {

    var wave, waveX = 2
    var bootCount, bootCountX = 50
    var patchCount, patchCountX = 5
    var mu, muX = 1.0
    var sigma, sigmaX = 1.3
    var vertices, verticesX = 1000
    var sx, sxX: Int = 10
    var px, pxX = 100
    val options = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
    val graph: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", options))
    val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    println("Graph created.")


    for (x <- 1 to 20) {
      println("wave ")
      val options = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
      val graph: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", options))
      val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
      println("Graph created.")

    for (wave <- 1 to 4 by 1) {
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    wave = waveX
    }
    println("px")
    for (px <- 10 to 100 by 10) {
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    px = pxX
    println("sx  ")
    for (sx <- 1 to 300 by 15) {
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    sx = sxX

    println("boot  ")
    for (bootCount <- 1 to 500 by 50) {
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    bootCount = bootCountX


    println("mu  ")
    for (mu <- 1.0 to 3.0 by 0.1) {
      val opX = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
      val grX: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", opX))
      val degX: Map[Int, Int] = grX.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, grX, degX, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    mu = muX

    println("sigma  ")
    for (mu <- 1.0 to 3 by 0.1) {
      for (sigma <- 0.0 to 2.0 by 0.1) {
        val opX = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
        val grX: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", opX))
        val degX: Map[Int, Int] = grX.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap

        val txt = GraphBootApproach2.graphBootAllSubGraph(px, grX, degX, sc, sx, patchCount, wave, bootCount)
        fw.write(wave + "\t" + mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
        fw.flush()
      }
    }
    mu = muX
    sigma = sigmaX
    println("vertices ")
    for (vertices <- 10000 to 100000 by 10000) {
      val opX = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
      val grX: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", opX))
      val degX: Map[Int, Int] = grX.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, grX, degX, sc, sx, patchCount, wave, bootCount)
      fw.write(wave + "\t" + mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    vertices = verticesX

  }
}
