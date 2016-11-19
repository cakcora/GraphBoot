import breeze.stats
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

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
    val wave: Int = 2
    val bootCount: Int = 10
    val patchCount:Int = 10

    val graph: Graph[Int, Int] = synthGraphGenerator(sc, "grid")
    var seedCount: Int = (graph.numVertices / 2).toInt
    if (seedCount > 5000) seedCount = 5000;
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val C: Double = 2
    for(j<-1 to patchCount){
      val seeds: RDD[(VertexId, Int)] = chooseSeeds(sc, graph, seedCount)
      var initialGraph: Graph[Int, Int] = graph.mapVertices((id, _) => 1500)
      initialGraph = initialGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
      val subGraph: Graph[Int, Int] = subgraphWithWave(initialGraph, wave)
      val proxySampleSize: Int = 1 + (subGraph.numVertices / 2).toInt
      println("Picking nodes " + proxySampleSize + " times:")

      var bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()
      println("**************BOOTSTRAPPING**********")
      val vertexList: List[Int] = subGraph.vertices.collect().map(x => x._1.toInt).toList
      val listLength: Int = vertexList.length
      val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap

      val seedSet: Set[Int] = seeds.map(e => e._1.toInt).collect().toSet
      for (i <- 1 to bootCount) {
        val kSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
        val kNonSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
        val random: Random = new Random()
        for (j <- 1 to proxySampleSize) {
          val chosen: Int = vertexList(random.nextInt(listLength))
          if (seedSet(chosen)) {
            kSeedMap(degrees(chosen)) += 1
          }
          else {
            kNonSeedMap(degrees(chosen)) += 1
          }
        }
        val numSeeds = kSeedMap.map(e => e._2.toInt).sum
        val numNonSeeds = kNonSeedMap.map(e => e._2.toInt).sum
        var avgDegree = 0.0
        val p0 = kSeedMap(0) / numSeeds
        for (i <- (kSeedMap ++ kNonSeedMap)) {
          val i1: Double = kSeedMap(i._1) +  Math.abs(1 - p0) * kNonSeedMap(i._1)
          avgDegree += i._1 * i1 / ((numSeeds +numNonSeeds))
        }
        //add avg degree from this bootstrap
        bstrapDegrees += avgDegree
        println(i + "th boothstrap: avgDegree " + avgDegree)
      }
      patchDegrees +=breeze.stats.mean(bstrapDegrees);
    }
    var denom1: Double = math.pow(patchCount, 0.5)
    val valbase: Double = (C / denom1) * Math.pow(Math.pow(patchDegrees.map(e=>e*e).sum,0.5)/patchCount,0.5)
    val i1 = breeze.stats.mean(patchDegrees) - valbase
    val i2 = breeze.stats.mean(patchDegrees) + valbase

    val avgGraphDeg: Double = breeze.stats.mean(graph.degrees.map(_._2.toDouble).collect())
    println("Bootstrap started with " + seedCount + " seeds, in a graph of " + graph.numVertices + " vertices, " + graph.numEdges + " edges.")
    println("Within the interval[" + i1 + " , " + i2 + "]:" + (avgGraphDeg > i1 && avgGraphDeg < i2) + ", with a mean and variance of " + avgGraphDeg + ", " + breeze.stats.variance)
    sc.stop()
  }


  def synthGraphGenerator(sc: SparkContext, graphType: String): Graph[Int, Int] = {

    graphType match {
      case "grid" => {
        val g: Graph[(Int, Int), Double] = GraphGenerators.gridGraph(sc, 10, 10)
        val gra: Graph[Int, Int] = g.mapVertices((a, b) => 1).mapEdges(a => 1)
        GraphCleaning.removeMultipleEdges(sc, gra)
      }
      case "lognormal" => {
        val gr: Graph[Long, Int] = GraphGenerators.logNormalGraph(sc, 10000, 1, 300, 10).removeSelfEdges()
        GraphCleaning.removeMultipleEdges(sc, gr.mapVertices((a, b) => a.toInt))
      }
      case "rmat" =>{
        GraphGenerators.rmatGraph(sc,1000, 15000)
      }
      case "dblp" => {
        GraphLoader.edgeListFile(sc, "src/main/resources/dblpgraph.txt")
      }
      case _: String => {
        println("No preference for graph type: Using a random star graph.")
        GraphGenerators.starGraph(sc, 100)
      }
    }
  }

  def subgraphWithWave(initialGraph: Graph[Int, Int], wave: Int): Graph[Int, Int] = {
    val dist = initialGraph.pregel(Int.MaxValue)(
      (id, dist, newDist) => Math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
    val subGraph = dist.subgraph(vpred = ((vertexId, vertexDistance) => {
      vertexDistance <= wave
    }))
    subGraph
  }

  def chooseSeeds(sc: SparkContext, graph: Graph[Int, Int], seedCount: Int): RDD[(graphx.VertexId, Int)] = {
    val vList: List[Int] = graph.vertices.collect().map(x => x._1.toInt).toList
    val size = vList.length
    val random = new Random()
    var seedList: ListBuffer[(Long, Int)] = ListBuffer()
    for (i <- 1 to seedCount) {
      seedList += Tuple2(vList(random.nextInt(size)), 1)
    }
    sc.parallelize(seedList)
  }


}
