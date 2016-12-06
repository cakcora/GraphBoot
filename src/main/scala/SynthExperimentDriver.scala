import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
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
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger().setLevel(Level.ERROR)
    val fw = new FileWriter("approach2.txt");
    val header = "wave\tmu\tsigma\tvertices\tseedCount\tbootCount\tpatchCount\tpx\tnumVertices\tnumEdges\tmean\tavgGraphDeg\tvarianceOfpatchDegrees\tC\n"
    fw.write(header);

    var wave, waveX = 2
    var bootCount, bootCountX = 10
    var patchCount, patchCountX = 10
    var mu, muX = 1.0
    var sigma, sigmaX = 1.3
    var vertices, verticesX = 10000
    var sx, sxX: Int = 10
    var px, pxX = 40
    val options = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
    val graph: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", options))
    val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    println("Graph created.")

    for (patchCount <- 1 to 10 by 1) {
      println("patch: " + patchCount)
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave + "\t" + mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    patchCount = patchCountX
    for (wave <- 1 to 4 by 1) {
      println("wave: " + wave)
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    wave = waveX
    for (px <- 10 to 100 by 10) {
      println("px: " + px)
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    px = pxX
    for (sx <- 1 to 300 by 15) {
      println("sx: " + sx)
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    sx = sxX

    for (bootCount <- 1 to 500 by 50) {
      println("boot: " + bootCount)
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    bootCount = bootCountX
    for (v <- 1 to 10)
    for (mu <- 1.0 to 3.0 by 0.5) {
      println("mu: " + mu)
      val options3 = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
      val graph3: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", options3))
      val degrees3: Map[Int, Int] = graph3.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, graph, degrees3, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    mu = muX
    for (v <- 1 to 10)
    for (sigma <- 0.0 to 2.0 by 0.2) {
      println("sigma: " + sigma)
      val opX = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
      val grX: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", opX))
      val degX: Map[Int, Int] = grX.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap

      val txt = GraphBootApproach2.graphBootAllSubGraph(px, grX, degX, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    sigma = sigmaX
    for (vertices <- 10000 to 100000 by 10000) {
      println("vertices: " + vertices)
      val opX = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
      val grX: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", opX))
      val degX: Map[Int, Int] = grX.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
      val txt = GraphBootApproach2.graphBootAllSubGraph(px, grX, degX, sc, sx, patchCount, wave, bootCount)
      fw.write(wave+"\t"+mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    vertices = verticesX
    fw.close()
    sc.stop()
  }
}
