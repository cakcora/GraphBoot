import breeze.stats.DescriptiveStats
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx._
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
    val options = Map(("mu", 1.0), ("sigma",0.005), ("vertices",100))
    val graph: Graph[Int, Int] = GraphCleaning.cleanGraph(sc,SyntheticData.synthGraphGenerator(sc,"lognormal", options))
    var seedCount: Int = (graph.numVertices / 2).toInt
    if (seedCount > 200) seedCount = 200;
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    val C: Double = 2
    val degrees: Map[PartitionID, PartitionID] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    for(j<-1 to patchCount){
      val seeds: RDD[(VertexId, PartitionID)] = chooseSeeds(sc, graph, seedCount)
      var initialGraph: Graph[PartitionID, PartitionID] = graph.mapVertices((id, _) => 1500)
      initialGraph = initialGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
      val subGraph: Graph[PartitionID, PartitionID] = subgraphWithWave(initialGraph, wave)
      val proxySampleSize: PartitionID = 1 + (subGraph.numVertices / 2).toInt


      val vertexList: List[PartitionID] = subGraph.vertices.collect().map(x => x._1.toInt).toList

      val seedSet: Set[PartitionID] = seeds.map(e => e._1.toInt).collect().toSet
      val bstrapDegrees: List[Double] = BootStrapper.boot(bootCount, proxySampleSize, vertexList, degrees, seedSet)
    val dc = (i: Double) => { DescriptiveStats.percentile(bstrapDegrees, i) }
    val length: Double = 0.5*(dc(0.95) -dc(0.05))
    val M: Double = dc(0.5)
    patchDegrees +=M;
    intervalLengths+= length
    println("Patch M:" + M+ " W:"+length)
  }
    val denom: Double = math.pow(patchCount, 0.5)
    val nom: Double = Math.pow(intervalLengths.map(e=>e*e).sum/ patchCount, 0.5)
    val mean: Double = breeze.stats.mean(patchDegrees)

    val collect: List[Double] = degrees.map(e=>e._2.toDouble).toList
    val avgGraphDeg: Double = breeze.stats.mean(collect)
    val cexp = (mean*denom -avgGraphDeg*denom)/nom
    println("Bootstrap started with " + seedCount + " seeds, in a graph of " + graph.numVertices + " vertices, " + graph.numEdges + " edges.")
    println("boot mean: "+mean+", with a true mean " + avgGraphDeg + ", boot variance " + breeze.stats.variance(patchDegrees))
    println("c :"+cexp)
    sc.stop()
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
