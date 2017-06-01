import breeze.stats.DescriptiveStats
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBoot {


  def compute(sc: SparkContext, graph: Graph[Int, Int], degrees: Map[Int, Int], seedArray: Array[(VertexId, Int)], expOptions: Map[String, Int], method: String): Map[String, AnyVal] = {
    val wave = expOptions("wave")
    val bootCount = expOptions("bootCount")
    val seeds: RDD[(Long, Int)] = sc.makeRDD(seedArray)

    val lmsiList = if (method == "parScala") {
      LMSI.parallelLMSI(graph, seeds, wave)
    }
    else if (method == "seq")
      LMSI.sequentialLMSI(sc, graph, seedArray, wave)
    else if (method == "parSpark")
      LMSI.parallelLMSISparkified(graph, seeds, wave)
    else {
      throw new IllegalArgumentException("method selection is wrong with " + method)
    }

    val bstrapDegrees: List[Double] = boot(bootCount, lmsiList, degrees, seeds)

    val M: Double = breeze.stats.mean(bstrapDegrees)

    val collect: List[Double] = degrees.map(e => e._2.toDouble).toList
    val avgGraphDeg: Double = breeze.stats.mean(collect)
    val medGraphDeg: Double = {
      val s = collect.size
      val sorts = collect.sorted.toIndexedSeq
      if (s % 2 == 1) {
        sorts(s / 2)
      }
      else {
        (sorts(1 + s / 2) + sorts(s / 2)) / 2
      }
    }
    val dc = (i: Double) => {
      DescriptiveStats.percentile(bstrapDegrees, i)
    }
    val l1: Double = dc(0.05)
    val lmin: Double = bstrapDegrees.min(Ordering.Double)
    val l2: Double = dc(0.95)
    val lmax: Double = bstrapDegrees.max(Ordering.Double)
    val vertexPercentage = lmsiList.length.toDouble / graph.numVertices
    val uniqueVertexPercentage = lmsiList.distinct.size.toDouble / graph.numVertices
    val txt: Map[String, AnyVal] = Map(("lmsiAll", vertexPercentage), ("lmsiDistinct", uniqueVertexPercentage), ("edges", graph.numEdges), ("mean", M), ("medGraphDeg", medGraphDeg), ("avgGraphDeg", avgGraphDeg), ("varianceOfBootStrapDegrees", breeze.stats.variance(bstrapDegrees)), ("l1", l1), ("l2", l2),
      ("lmin", lmin), ("lmax", lmax))


    return txt

  }


  def boot(bootCount: Int, candidateList: List[Int], degrees: Map[Int, Int], seeds: RDD[(VertexId, Int)]): List[Double] = {
    val bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()

    val seedList: List[Int] = seeds.map(e => e._1.toInt).collect().toList
    val nSeedLength: Int = candidateList.length
    val seedLength: Int = seedList.length
    val probs: mutable.ArrayBuffer[(Int, Int)] = reverseProbArray(candidateList, degrees)
    val inter: Int = probs(probs.size - 1)._2

    for (i <- 1 to bootCount) {
      val kSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)

      val random = scala.util.Random
      for (j <- 1 to seedLength) {
        val chosenSeed: Int = seedList(random.nextInt(seedLength))
        kSeedMap(degrees(chosenSeed)) += 1

      }
      val kNonSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      for (j <- 1 to nSeedLength) {
        val chosenNseed: Int = findInProbArray(probs, random.nextInt(inter)) // inverse degree probability sampling
        kNonSeedMap(degrees(chosenNseed)) += 1
      }

      var avgDegree = 0.0
      var p0 = 0;
      if (kSeedMap(0) != 0) p0 = kSeedMap(0) / seedLength

      for (i <- (kSeedMap.keySet ++ kNonSeedMap.keySet)) {
        val i1: Double = kSeedMap(i) + Math.abs(1 - p0) * kNonSeedMap(i)
        avgDegree += i * i1 / (seedLength + nSeedLength)
      }
      //add avg degree from this bootstrap
      bstrapDegrees += avgDegree
    }
    bstrapDegrees.toList
  }


  def reverseProbArray(candidateList: List[Int], degrees: Map[Int, Int]): ArrayBuffer[(Int, Int)] = {
    val probs = mutable.ArrayBuffer[(Int, Int)]()
    var inter = 0
    val multiplier = 1 + degrees.map(e => e._2).max
    //temporarily using 1000 instead of multiplier
    for (v <- candidateList) {
      val j = (1000 * (1.0 / degrees(v))).toInt
      if (degrees(v) != 0 && j < 1) {
        probs += Tuple2(v, inter + 1)
        // assigning a prob of 1 to nodes with degree>1000
        inter += 1
      }
      else {
        probs += Tuple2(v, inter + j)
        inter += j
      }
    }

    return (probs)
  }


  def findInProbArray(p3: ArrayBuffer[(Int, Int)], target: Int): Int = {

    var left = 0
    var right = p3.length - 1
    if (target > p3(right)._2) return -1
    while (p3(left)._2 < target) {
      val mid = left + (right - left) / 2
      if (p3(left)._2 >= target) {
        return p3(left)._1
      }
      if (p3(mid)._2 < target) {
        left = mid + 1
      }
      else if (p3(mid)._2 >= target) {
        right = mid
      }
    }
    return p3(left)._1

  }

}
