import java.io.FileWriter

import breeze.stats.DescriptiveStats
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBootApproach1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[16]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger().setLevel(Level.ERROR)
    val fw = new FileWriter("approach1.txt");
    val header = "mu\\tsigma\\tvertices\\tseedCount\\tbootCount\\tpatchCount\\tpx\\tnumVertices\\tnumEdges\\tmean\\tavgGraphDeg\\tvarianceOfpatchDegrees\\tC\n"
    fw.write(header);

    val wave: Int = 3
    val bootCount: Int = 10
    val patchCount: Int = 10
    var mu = 1.0
    var sigma = 1.3
    var vertices = 10000
    var sx = 2
    var px = 2

    val options = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
    val graph: Graph[Int, Int] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", options))
    var seedCount: Int = (graph.numVertices / sx).toInt
    if (seedCount > 200) seedCount = 200;
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    val degrees: Map[PartitionID, PartitionID] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    for (j <- 1 to patchCount) {
      val seeds: RDD[(VertexId, PartitionID)] = Common.chooseSeeds(sc, graph, seedCount)
      var initialGraph: Graph[PartitionID, PartitionID] = Common.weightVertices(graph)
      initialGraph = initialGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
      val subGraph: Graph[PartitionID, PartitionID] = Common.subgraphWithWave(initialGraph, wave)
      val proxySampleSize: PartitionID = 1 + (subGraph.numVertices / px).toInt


      val vertexList: List[PartitionID] = subGraph.vertices.collect().map(x => x._1.toInt).toList

      val seedSet: Set[PartitionID] = seeds.map(e => e._1.toInt).collect().toSet
      val bstrapDegrees: List[Double] = BootStrapper.boot(bootCount, proxySampleSize, vertexList, degrees, seedSet)
      val dc = (i: Double) => {
        DescriptiveStats.percentile(bstrapDegrees, i)
      }
      val length: Double = 0.5 * (dc(0.95) - dc(0.05))
      val M: Double = dc(0.5)
      patchDegrees += M;
      intervalLengths += length
      println("Patch M:" + M + " W:" + length)
    }
    val txt = Common.results(patchCount, graph, seedCount, patchDegrees, intervalLengths, degrees)
    fw.write(options("mu") + "\t" + options("sigma") + "\t" + options("vertices") + "\t" + seedCount + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
    fw.flush()
    sc.stop()
  }


}
