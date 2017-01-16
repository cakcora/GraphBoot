import breeze.stats.DescriptiveStats
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks._

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBoot {


  def compute(sc: SparkContext, graph: Graph[Int, Int], degrees: Map[Int, Int], seedArray: Array[(VertexId, Int)], expOptions: Map[String, Int], method: String): Map[String, AnyVal] = {
    val wave = expOptions("wave")
    val bootCount = expOptions("bootCount")
    val seeds: RDD[(Long, Int)] = sc.makeRDD(seedArray)

    var lmsiList = if (method == "parScala") {
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


  def boot(bootCount: Int, candidateList: List[Int], degrees: Map[Int, Int], seeds: RDD[(VertexId, Int)]): List[Double] = {
    val bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()

    val seedList: List[Int] = seeds.map(e => e._1.toInt).collect().toList
    val nSeedLength: Int = candidateList.length
    val seedLength: Int = seedList.length
    val probMap: (mutable.ListMap[Int, Int], Int) = reverseProbMap(candidateList, degrees)
    val probs: mutable.ListMap[Int, Int] = probMap._1
    println(probs)
    val inter: Int = probMap._2

    for (i <- 1 to bootCount) {
      val kSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)

      val random = scala.util.Random
      for (j <- 1 to seedLength) {
        val chosenSeed: Int = seedList(random.nextInt(seedLength))
        kSeedMap(degrees(chosenSeed)) += 1

      }
      val kNonSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      for (j <- 1 to nSeedLength) {
        val chosenNseed: Int = pickWithProb(probs, random.nextInt(inter)) // inverse degree probability sampling
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
      println(avgDegree + " " + kNonSeedMap)
    }
    bstrapDegrees.toList
  }

  def pickWithProb(probs: mutable.ListMap[Int, Int], pro: Int): Int = {
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


  def reverseProbMap(candidateList: List[Int], degrees: Map[Int, Int]) = {
    val probs = mutable.ListMap.empty[Int, Int]
    var inter = 0
    for (v <- candidateList) {
      val j = (1000 * (1.0 / degrees(v))).toInt
      if (degrees(v) != 0 && j < 1) {
        println("illegal state")
      }
      probs.put(v, inter + j)
      inter += j
      println("putting " + v)
    }
    if (inter <= 0) throw new IllegalArgumentException("overflow in prob map computations")
    (probs, inter)
  }

  def reverseProbArray(candidateList: List[Int], degrees: Map[Int, Int]): ArrayBuffer[(Int, Int)] = {
    val probs = mutable.ArrayBuffer[(Int, Int)]()
    var inter = 0
    var i = 0
    for (v <- candidateList) {
      val j = (1000 * (1.0 / degrees(v))).toInt
      if (degrees(v) != 0 && j < 1) {
        println("illegal state")
      }
      probs += Tuple2(v, inter + j)
      inter += j
      i += 1
    }
    return (probs)
  }

  def pickWithProbArray(probs: mutable.ArrayBuffer[(Int, Int)], pro: Int): Int = {
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

}
