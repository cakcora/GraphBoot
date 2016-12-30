import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
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

    val degrees: Map[Int, Int] = Map((1, 20), (2, 20), (3, 20), (4, 2), (5, 2), (6, 2), (7, 2))

    val fgResult: (mutable.LinkedHashMap[Int, Int], Int) = fg(degrees)
    val probs: mutable.LinkedHashMap[Int, Int] = fgResult._1
    var inter: Int = fgResult._2
    val ms = mutable.Map.empty[Int, Int].withDefaultValue(0)

    val random = new Random()
    for (s <- 1 to 1000) {
      val pro = random.nextInt(inter)
      var chosen: Int = -1

      breakable {
        for (i <- probs) {
          if (pro <= i._2) {
            chosen = i._1
            ms(i._1) += 1
            break
          }

        }
      }

    }
    println(ms)
  }

  private def fg(degrees: Map[PartitionID, PartitionID]) = {
    val probs = mutable.LinkedHashMap.empty[PartitionID, PartitionID]
    var sum: Double = degrees.map(e => e._2).sum
    var inter = 0
    for (v <- degrees) {
      val j = (10000 * (sum / v._2)).toInt
      if (v._2 != 0 && j < 1) {
        println("wrong")
      }
      probs.put(v._1, inter + j)
      inter += j
    }
    (probs, inter)
  }
}
