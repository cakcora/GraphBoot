import breeze.stats.DescriptiveStats
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBootApproach3 {


  def graphBootAllSubGraph(px: Int, graph: Graph[Int, Int], degrees: Map[Int, Int], sc: SparkContext, sx: Int, wave: Int, bootCount: Int): String = {
    val seedCount: PartitionID = sx
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()

    val seeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, seedCount)

    val weightedGraph: Graph[PartitionID, PartitionID] = Common.weightVertices(graph)
    val lis = seeds.map(e => e._1.toInt).collect().toList
    val initialGraph = weightedGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))

    val fut: Future[List[List[Int]]] = Future.traverse(lis) { i =>
      val localEdges: RDD[Edge[Int]] = Common.findWaveEdges(initialGraph, i, wave)
      Future {
        LMSI.singleSeed(localEdges, i, wave)
      }
    }

    val subList = Await.result(fut, Duration.Inf).flatten

    val bstrapDegrees: List[Double] = BootStrapper.boot(bootCount, px, subList, degrees, seeds)

    val dc = (i: Double) => {
      DescriptiveStats.percentile(bstrapDegrees, i)
    }
    val length: Double = 0.5 * (dc(0.95) - dc(0.05))
    val M: Double = dc(0.5)
    patchDegrees += M;
    intervalLengths += length

    val txt = Common.results(patchCount, graph, seedCount, patchDegrees, intervalLengths, degrees)
    return txt

  }


}
