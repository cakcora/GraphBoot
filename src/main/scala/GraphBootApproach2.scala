import breeze.stats.DescriptiveStats
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, graphx}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBootApproach2 {


  def graphBoot(px: Int, graph: Graph[Int, Int], degrees: Map[Int, Int], sc: SparkContext, sx: Int, patchCount: Int, wave: Int, bootCount: Int): String = {
  println(wave+" wave")
    val seedCount: PartitionID = 2//(graph.numVertices *sx/100).toInt
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    for (j <- 1 to patchCount) {
      val seeds: RDD[(VertexId, Int)] = chooseSeeds(sc, graph, seedCount)

      val weightedGraph: Graph[Int, Int] = graph.mapVertices((id, _) => 1500)
      val lis = seeds.map(e => e._1.toInt).collect().toList
      //      var time1 = System.currentTimeMillis()
      var initialGraph = weightedGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
      val subGraph: Graph[Int, Int] = subgraphWithWave(initialGraph, wave)
      val aList: Set[Edge[Int]] = subGraph.edges.collect().toSet
      var mList: mutable.Set[Pair[Int, Int]] = new mutable.HashSet[Pair[Int, Int]]()
      for (a <- aList) {
        mList.add((a.srcId.toInt, a.dstId.toInt))
      }
      val fut:Future[List[List[PartitionID]]] = Future.traverse(lis) { i =>
        Future {
          LMSI.singleSeed(mList.clone(), i, wave)
        }
      }

      val subList = Await.result(fut, Duration.Inf).flatten
//      println(System.currentTimeMillis()-time1+" ms in lmsi")
      val proxySampleSize: PartitionID = 1 + (subList.size * px/100).toInt

      val seedSet: Set[PartitionID] = seeds.map(e => e._1.toInt).collect().toSet
//      var time2 = System.currentTimeMillis()
      val bstrapDegrees: List[Double] = BootStrapper.boot(bootCount, proxySampleSize, subList, degrees, seedSet)
//      println(System.currentTimeMillis()-time2+" ms in boot")

      val dc = (i: Double) => {
        DescriptiveStats.percentile(bstrapDegrees, i)
      }
      val length: Double = 0.5 * (dc(0.95) - dc(0.05))
      val M: Double = dc(0.5)
      patchDegrees += M;
      intervalLengths += length
//      println("Patch M:" + M + " W:" + length)
    }
    val txt = Common.results(patchCount, graph, seedCount, patchDegrees, intervalLengths, degrees)
    return txt

  }


  def subgraphWithWave(initialGraph: Graph[Int, Int], wave: Int): Graph[Int, Int] = {
    val dist = initialGraph.pregel(Int.MaxValue)(
      (id, dist, newDist) => Math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        }
        if (triplet.dstAttr + triplet.attr < triplet.srcAttr) {
          Iterator((triplet.srcId, triplet.dstAttr + triplet.attr))
        }else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
    val subGraph = dist.subgraph(vpred = ((vertexId, vertexDistance) => {
      vertexDistance <= wave
    }))
    println("subgraph"+subGraph.vertices.collect().mkString(" "))
    subGraph
  }


  def chooseSeeds(sc: SparkContext, graph: Graph[Int, Int], seedCount: Int): RDD[(graphx.VertexId, Int)] = {
    val vList: List[Int] = graph.vertices.collect().map(x => x._1.toInt).toList
    val size = vList.length
    val random = new Random()
    var seedList: ListBuffer[(Long, Int)] = ListBuffer()
    for (i <- 1 to seedCount) {
      val tuple: (VertexId, PartitionID) = Tuple2(vList(random.nextInt(size)), 0)
      seedList += tuple
    }
    sc.parallelize(seedList)
  }


}
