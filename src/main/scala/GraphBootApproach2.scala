import breeze.stats.DescriptiveStats
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBootApproach2 {


  def graphBoot(px: Int, graph: Graph[Int, Int], degrees: Map[Int, Int], sc: SparkContext, sx: Int, patchCount: Int, wave: Int, bootCount: Int): String = {
    val seedCount: PartitionID = (graph.numVertices * sx / 100).toInt
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    for (j <- 1 to patchCount) {
      val seeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, seedCount)

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
      val proxySampleSize: PartitionID = 1 + (subList.size * px/100).toInt

      val seedSet: Set[PartitionID] = seeds.map(e => e._1.toInt).collect().toSet
//      var time2 = System.currentTimeMillis()
      val bstrapDegrees: List[Double] = BootStrapper.boot(bootCount, proxySampleSize, subList, degrees, seedSet)

      val dc = (i: Double) => {
        DescriptiveStats.percentile(bstrapDegrees, i)
      }
      val length: Double = 0.5 * (dc(0.95) - dc(0.05))
      val M: Double = dc(0.5)
      patchDegrees += M;
      intervalLengths += length
    }
    val txt = Common.results(patchCount, graph, seedCount, patchDegrees, intervalLengths, degrees)
    return txt

  }


  def subgraphWithWave(initialGraph: Graph[Int, Int], wave: Int): Graph[Int, Int] = {
    val dist = initialGraph.pregel(150)(
      (id, dist, newDist) => Math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        }
        else if (triplet.dstAttr + triplet.attr < triplet.srcAttr) {
          Iterator((triplet.srcId, triplet.dstAttr + triplet.attr))
        }
        else {
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


}
