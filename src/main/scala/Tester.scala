import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/15/2016.
  */
object Tester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("sd")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext


    val re: RDD[(Long, Long)] = sc.parallelize(Array(
      (0L, 1L), (0L, 2L),
      (0L, 3L), (0L, 4L),
      (0L, 5L), (1L, 6L),
      (2L, 8L), (6L, 7L),
      (2L, 7L),(3L, 9L),(3L,14L),
      (8L, 9L), (4L, 10L),
      (5L, 12L), (11L, 12L),
      (10L, 11L), (10L, 13L)
    ))
    val tupleGraph:Graph[Int,Int] = Graph.fromEdgeTuples(re, defaultValue = 15)
    val k= multipleInclusion(tupleGraph)
    println(k.mkString(" "))
  }

  def multipleInclusion(graph: Graph[Int, Int]): List[Int] = {
    var aList: Set[Edge[Int]] = graph.edges.collect().toSet
    var mList: mutable.Set[Tuple2[Int, Int]] = new mutable.HashSet[Tuple2[Int, Int]]()
    for (a <- aList) {
      mList.add((a.srcId.toInt, a.dstId.toInt))
    }

    val disc: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    disc.add(0)
    var wave = 0;


    val seen: ListBuffer[Int] = new ListBuffer[Int]()
    seen.append(0)
    while (mList.size != 0) {
      val phase: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      wave += 1
      val seenSet: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      var rList: mutable.Set[Tuple2[Int, Int]] = new mutable.HashSet[Tuple2[Int, Int]]()
      for ((a, b) <- mList) {
        var f = false
        if (disc.contains(a)) {
          f = true
          if (disc.contains(b)) {
            seen.append(b)
            seen.append(a)
          }
          else {
            phase.add(b)
            seenSet.add(b)
          }

        }
        else if (disc.contains(b)) {
          f = true
          phase.add(a)
          seenSet.add(a)

        }

        if (f) {
          rList.add((a, b))
        }
      }
      seen ++=seenSet
      disc ++= phase
      mList--=rList
    }
    seen.toList
  }


}
