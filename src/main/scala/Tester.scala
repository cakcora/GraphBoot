import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
  * Created by cxa123230 on 11/15/2016.
  */
object Tester {
  def findWaveEdges(graph: Graph[Int, Int], seed: Int, wave: Int): RDD[Edge[Int]] = {
    //    if(wave<1) new SparkException("You requested an in valid subgraph, wave="+wave)
    var neigh0: Array[VertexId] = Array(seed)
    var d: RDD[Edge[Int]] = graph.edges.filter(e => (seed == e.dstId) || (seed == e.srcId))
    for (v <- 2 to wave) {
      neigh0 = d.flatMap(e => List(e.srcId, e.dstId)).distinct().collect()
      d = graph.edges.filter(e => neigh0.contains(e.dstId) || neigh0.contains(e.srcId))
    }
    //    println(wave+" " +d.collect().mkString(" "))
    return d
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("sd")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val options = Map(("mu", 2.0), ("sigma", 1.3), ("vertices", 100))
    val fw = new FileWriter("waveApp3.txt", true);
    val gr: Graph[Int, Int] = SyntheticData.synthGraphGenerator(sc, "lognormal", options)
    val g: Graph[Int, Int] = SyntheticData.getTestGraph(sc)
    val graph = GraphCleaning.cleanGraph(sc, gr)

    println(gr.numVertices + " " + gr.numEdges)
    println(graph.numVertices + " " + graph.numEdges)




    val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap

    for (wave <- 1 to 4) {
      var t = System.currentTimeMillis()
      val txt = GraphBootApproach2.graphBootAllSubGraph(100, graph, degrees, sc, 10, 10, wave, 50)
      fw.write(wave + "\t" + 2 + "\t" + 1.3 + "\t" + 5 + "\t" + 100 + "\t" + 10 + "\t" + 3 + "\t" + 100 + "\t" + txt + "\n")
      fw.flush()
      println(System.currentTimeMillis() - t)
    }
    fw.close()
  }


}
