import java.util.concurrent.ThreadLocalRandom

import breeze.stats.DescriptiveStats
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBootPatchless {


  def graphBoot(sc: SparkContext, graph: Graph[Int, Int], degrees: Map[Int, Int], seedArray: Array[(VertexId, Int)], expOptions: Map[String, Int]): Map[String, AnyVal] = {
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    val wave = expOptions("wave")
    val bootCount = expOptions("bootCount")
    val bootSamplePercentage = expOptions("bootSamplePercentage")

    val weightedGraph: Graph[Int, Int] = Common.weightVertices(graph)
    val seeds = sc.makeRDD(seedArray)
    val seedList = seeds.map(e => e._1.toInt).collect().toList
    val initialGraph = weightedGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
    var t = System.currentTimeMillis()
    val k: Graph[PartitionID, PartitionID] = Common.subgraphWithWave(graph, wave)
    val newLmsiList = LMSI.multipleSeeds(k, seedList, wave)
    //    val fut: Future[List[List[Int]]] = Future.traverse(seedList) { i =>
    //      val localEdges: RDD[Edge[Int]] = Common.findWaveEdges(initialGraph, i, wave)
    //      Future {
    //        LMSI.singleSeed(localEdges, i, wave)
    //      }
    //    }

    //    val lmsiList = Await.result(fut, Duration.Inf).flatten
    //println(System.currentTimeMillis()-t+" seconds passed in the old approach")
    //(println(lmsiList))
    val bstrapDegrees: List[Double] = boot(bootCount, bootSamplePercentage, newLmsiList, degrees, seeds)

    val M: Double = breeze.stats.mean(bstrapDegrees)

    val collect: List[Double] = degrees.map(e => e._2.toDouble).toList
    val avgGraphDeg: Double = breeze.stats.mean(collect)

    val dc = (i: Double) => {
      DescriptiveStats.percentile(bstrapDegrees, i)
    }
    val l1: Double = dc(0.05)
    val lmin: Double = bstrapDegrees.min(Ordering.Double)
    val l2: Double = dc(0.95)
    val lmax: Double = bstrapDegrees.max(Ordering.Double)

    val txt: Map[String, AnyVal] = Map(("vertices", graph.numVertices), ("edges", graph.numEdges), ("mean", M), ("avgGraphDeg", avgGraphDeg), ("varianceOfBootStrapDegrees", breeze.stats.variance(bstrapDegrees)), ("l1", l1), ("l2", l2),
      ("lmin", lmin), ("lmax", lmax))


    return txt

  }


  def boot(bootCount: Int, bootSamplePercentage: Int, candidateList: List[Int], degrees: Map[Int, Int], seeds: RDD[(VertexId, Int)]): List[Double] = {
    val bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()

    val seedList: List[Int] = seeds.map(e => e._1.toInt).collect().toList
    val nSeedLength: Int = candidateList.length
    val seedLength: Int = seedList.length
    val probMap: (mutable.LinkedHashMap[Int, Int], Int) = reverseProbMap(candidateList, degrees)
    val probs: mutable.LinkedHashMap[Int, Int] = probMap._1
    val inter: Int = probMap._2

    for (i <- 1 to bootCount) {
      val kSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      val kNonSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      val random: ThreadLocalRandom = ThreadLocalRandom.current()
      val random2: ThreadLocalRandom = ThreadLocalRandom.current()
      for (j <- 1 to seedLength) {
        val chosenSeed: Int = seedList(random2.nextInt(seedLength))
        kSeedMap(degrees(chosenSeed)) += 1

      }
      for (j <- 1 to nSeedLength) {
        //        val chosenNSeed: Int = seedList(random2.nextInt(seedLength))// uniform probability sampling
        val chosenNseed: Int = pickWithProb(probs, random.nextInt(inter)) // inverse degree probability sampling
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

  private def pickWithProb(probs: mutable.LinkedHashMap[Int, Int], pro: Int): Int = {
    var pickedKey = -1
    breakable {
      for (i <- probs) {
        if (pro <= i._2) {
          pickedKey = i._1
          break
        }
      }
    }
    pickedKey
  }


  private def reverseProbMap(candidateList: List[Int], degrees: Map[Int, Int]) = {
    val probs = mutable.LinkedHashMap.empty[Int, Int]
    var sum: Double = degrees.map(e => e._2).sum
    var inter = 0
    for (v <- candidateList) {
      val j = (1000 * (1.0 / degrees(v))).toInt
      if (degrees(v) != 0 && j < 1) {
        println("wrong")
      }
      probs.put(v, inter + j)
      inter += j
    }
    if (inter <= 0) throw new IllegalArgumentException("overflow in prob map computations")
    (probs, inter)
  }

}
