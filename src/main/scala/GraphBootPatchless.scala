import java.util.concurrent.ThreadLocalRandom

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
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    val seedCount = expOptions("seedCount")
    val wave = expOptions("wave")
    val bootCount = expOptions("bootCount")
    val bootSamplePercentage = expOptions("bootSamplePercentage")

    val seeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, seedCount)

    val weightedGraph: Graph[Int, Int] = Common.weightVertices(graph)
    val lis = seeds.map(e => e._1.toInt).collect().toList
    val initialGraph = weightedGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))

    val fut: Future[List[List[Int]]] = Future.traverse(lis) { i =>
      val localEdges: RDD[Edge[Int]] = Common.findWaveEdges(initialGraph, i, wave)
      Future {
        LMSI.singleSeed(localEdges, i, wave)
      }
    }

    val lmsiList = Await.result(fut, Duration.Inf).flatten

    val bstrapDegrees: List[Double] = boot(bootCount, bootSamplePercentage, lmsiList, degrees, seeds)

    val M: Double = breeze.stats.mean(bstrapDegrees)

    val collect: List[Double] = degrees.map(e => e._2.toDouble).toList
    val avgGraphDeg: Double = breeze.stats.mean(collect)

    val txt = (graph.numVertices + "\t" + graph.numEdges + "\t") + M + "\t" + avgGraphDeg + "\t" + breeze.stats.variance(bstrapDegrees)
    return txt

  }


  def boot(bootCount: Int, bootSamplePercentage: Int, candidateList: List[Int], degrees: Map[Int, Int], seeds: RDD[(VertexId, Int)]): List[Double] = {
    val bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val seedList: List[Int] = seeds.map(e => e._1.toInt).collect().toList
    val proxySampleSize = (candidateList.size * bootSamplePercentage / 100.0).toInt
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
      for (i <- (kSeedMap.keySet ++ kNonSeedMap.keySet)) {
        val i1: Double = kSeedMap(i) + Math.abs(1 - p0) * kNonSeedMap(i)
        avgDegree += i * i1 / ((numSeeds + numNonSeeds))
      }
      //add avg degree from this bootstrap
      bstrapDegrees += avgDegree
    }
    bstrapDegrees.toList
  }

}
