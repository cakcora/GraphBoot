import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by cxa123230 on 11/15/2016.
  */
object Tester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext

    val degrees: Map[Int, Int] = Map((1, 2), (2, 20), (3, 2), (4, 2), (5, 2), (6, 2), (7, 2))
    val candidateList: List[Int] = List(1, 3, 5)
    val probMap: (mutable.LinkedHashMap[Int, Int], Int) = reverseProbMap(candidateList, degrees)
    val probs: mutable.LinkedHashMap[Int, Int] = probMap._1
    val inter: Int = probMap._2

    val ms = mutable.Map.empty[Int, Int].withDefaultValue(0)

    val random = new Random()
    for (s <- 1 to 1000) {
      val pro = random.nextInt(inter)
      val picked = pickWithProb(probs, pro)
      println(picked + " " + degrees(picked))
    }
    println(ms)
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
      val j = (10000 * (sum / degrees(v))).toInt
      if (degrees(v) != 0 && j < 1) {
        println("wrong")
      }
      probs.put(v, inter + j)
      inter += j
    }
    (probs, inter)
  }
}
