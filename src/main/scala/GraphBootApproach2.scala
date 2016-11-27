import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, graphx}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBootApproach2 {



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
    val graph: Graph[Int, Int] = GraphCleaning.cleanGraph(sc,SyntheticData.synthGraphGenerator(sc,"lognormal",options))
    println("Graph created.")
    var seedCount: Int = (graph.numVertices / 2).toInt
    if (seedCount > 200) seedCount = 200;
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val C: Double = 2
    val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    for(j<-1 to patchCount){
      val seeds: RDD[(VertexId, Int)] = chooseSeeds(sc, graph, seedCount)
      var initialGraph: Graph[Int, Int] = graph.mapVertices((id, _) => 1500)
      initialGraph = initialGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
      val subGraph: Graph[Int, Int] = subgraphWithWave(initialGraph, wave)
      val subList: ListBuffer[Int] = new ListBuffer[Int]()
      var aList: Set[Edge[Int]] = subGraph.edges.collect().toSet
      var mList: mutable.Set[Tuple2[Int, Int]] = new mutable.HashSet[Tuple2[Int, Int]]()
      for (a <- aList) {
        mList.add((a.srcId.toInt, a.dstId.toInt))
      }
      for(seed<-seeds.map(e=>e._1.toInt).collect().toList){
        subList++= LMSI.singleSeed(mList.clone(),seed)
      }

      val proxySampleSize: Int = 1 + (subGraph.numVertices / 2).toInt
      var bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()
      println("\nPatch: : "+j)
      val vertexList: List[Int] = subList.toList
      val listLength: Int = vertexList.length

      val seedSet: Set[Int] = seeds.map(e => e._1.toInt).collect().toSet
      print("\tBoot: " )
      for (i <- 1 to bootCount) {
        print(" "+i)
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
        var p0 = kSeedMap(0)
        if(numSeeds==0){
          // no seed was sampled :(
          p0=0;
        }
        else p0= kSeedMap(0) / numSeeds
        var avgDegree = 0.0
        for (i <- (kSeedMap ++ kNonSeedMap)) {
          val i1: Double = kSeedMap(i._1) +  Math.abs(1 - p0) * kNonSeedMap(i._1)
          avgDegree += i._1 * i1 / ((numSeeds +numNonSeeds))
        }
        //add avg degree from this bootstrap
        bstrapDegrees += avgDegree
//        println(i + "th boothstrap: avgDegree " + avgDegree)
      }
      patchDegrees +=breeze.stats.mean(bstrapDegrees);
    }
    val denom: Double = math.pow(patchCount, 0.5)
    val valBase: Double = (C / denom) * Math.pow(Math.pow(patchDegrees.map(e=>e*e).sum,0.5)/patchCount,0.5)
    val i1 = breeze.stats.mean(patchDegrees) - valBase
    val i2 = breeze.stats.mean(patchDegrees) + valBase

    val avgGraphDeg: Double = breeze.stats.mean(graph.degrees.map(_._2.toDouble).collect())
    println("\nGraphBoot with " + seedCount + " seeds, in a graph of " + graph.numVertices + " vertices, " + graph.numEdges + " edges.")
    println("Within the interval[" + i1 + " , " + i2 + "]:" + (avgGraphDeg > i1 && avgGraphDeg < i2) + ", with a mean and variance of " + avgGraphDeg + ", " + breeze.stats.variance(patchDegrees))
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
      seedList += Tuple2(vList(random.nextInt(size)), 0)
    }
    sc.parallelize(seedList)
  }


}
