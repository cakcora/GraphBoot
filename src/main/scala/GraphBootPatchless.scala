import java.util.concurrent.ThreadLocalRandom

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
object GraphBootPatchless {


  def graphBoot(sc: SparkContext, graph: Graph[Int, Int], degrees: Map[Int, Int], expOptions: Map[String, Int]): String = {
    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    val seedCount = expOptions("seedCount")
    val wave = expOptions("wave")
    val bootCount = expOptions("bootCount")
    val bootSampleCount = expOptions("bootSampleCount")


    {
      //      println("patch " + j)
      val seeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, seedCount)

      val weightedGraph: Graph[Int, Int] = Common.weightVertices(graph)
      val lis = seeds.map(e => e._1.toInt).collect().toList
      val initialGraph = weightedGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
      //      val subGraph: Graph[Int, Int] = Common.subgraphWithWave(initialGraph, wave)

      val fut: Future[List[List[Int]]] = Future.traverse(lis) { i =>
        val localEdges: RDD[Edge[Int]] = Common.findWaveEdges(initialGraph, i, wave)
        Future {
          LMSI.singleSeed(localEdges, i, wave)
        }
      }

      val subList = Await.result(fut, Duration.Inf).flatten

      val bstrapDegrees: List[Double] = boot(bootCount, bootSampleCount, subList, degrees, seeds)

      val dc = (i: Double) => {
        DescriptiveStats.percentile(bstrapDegrees, i)
      }
      val length: Double = 0.5 * (dc(0.95) - dc(0.05))
      val M: Double = dc(0.5)
      patchDegrees += M;
      intervalLengths += length
    }
    val txt = results(1, graph, seedCount, patchDegrees, intervalLengths, degrees)
    return txt

  }

  def results(patchCount: Int, graph: Graph[Int, Int], seedCount: Int, patchDegrees: ListBuffer[Double], intervalLengths: ListBuffer[Double], degrees: Map[Int, Int]): String = {
    val denom: Double = math.pow(patchCount, 0.5)
    val nom: Double = Math.pow(intervalLengths.map(e => e * e).sum / patchCount, 0.5)
    val mean: Double = breeze.stats.mean(patchDegrees)

    val collect: List[Double] = degrees.map(e => e._2.toDouble).toList
    val avgGraphDeg: Double = breeze.stats.mean(collect)
    val cexp = (mean * denom - avgGraphDeg * denom) / nom
    val txt = (graph.numVertices + "\t" + graph.numEdges + "\t") + mean + "\t" + avgGraphDeg + "\t" + breeze.stats.mean(intervalLengths) + "\t" + breeze.stats.variance(patchDegrees) + "\t" + cexp
    return txt
  }

  def boot(bootCount: Int, px: Int, candidateList: List[Int], degrees: Map[Int, Int], seeds: RDD[(VertexId, Int)]): List[Double] = {
    val bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val seedList: List[PartitionID] = seeds.map(e => e._1.toInt).collect().toList
    val proxySampleSize = (candidateList.size * px / 100.0).toInt
    val nSeedLength: Int = candidateList.length
    val seedLength: Int = seedList.length
    for (i <- 1 to bootCount) {
      val kSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      val kNonSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      val random: ThreadLocalRandom = ThreadLocalRandom.current()
      val random2: ThreadLocalRandom = ThreadLocalRandom.current()
      for (j <- 1 to proxySampleSize) {
        val chosenNseed: Int = candidateList(random.nextInt(nSeedLength))
        val chosenSeed: Int = seedList(random2.nextInt(seedLength))
        kSeedMap(degrees(chosenSeed)) += 1
        kNonSeedMap(degrees(chosenNseed)) += 1
      }

      val numSeeds = kSeedMap.map(e => e._2.toInt).sum
      val numNonSeeds = kNonSeedMap.map(e => e._2.toInt).sum
      var avgDegree = 0.0
      var p0 = 0;
      if (numSeeds != 0) p0 = kSeedMap(0) / numSeeds
      for (i <- (kSeedMap ++ kNonSeedMap)) {
        val i1: Double = kSeedMap(i._1) + Math.abs(1 - p0) * kNonSeedMap(i._1)
        avgDegree += i._1 * i1 / ((numSeeds + numNonSeeds))
      }
      //add avg degree from this bootstrap
      bstrapDegrees += avgDegree
    }
    bstrapDegrees.toList
  }

}
