import org.apache.spark.graphx.{EdgeDirection, EdgeRDD, Graph}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/25/2016.
  */
object LMSI {

  def singleSeed(graph: Graph[Int, Int], seed: Int, wave: Int): List[Int] = {
    val mList: EdgeRDD[Int] = graph.edges

    var li: ListBuffer[(Int, Int)] = mList.map(e => (e.srcId.toInt, e.dstId.toInt)).collect().to[ListBuffer]
    val disc: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    val seenList: ListBuffer[Int] = new ListBuffer[Int]()
    seenList.append(seed)
    disc.add(seed)

    var w = 0;
    while (!li.isEmpty && w < wave) {
      val phase: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      w += 1
      val seenSet: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      var rList: mutable.Set[(Int, Int)] = new mutable.HashSet[(Int, Int)]()
      for ((a, b) <- li) {
        var f = false
        if (disc.contains(a)) {
          f = true
          if (disc.contains(b)) {
            seenList.append(b)
            seenList.append(a)
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
      seenList ++=seenSet
      disc ++= phase
      li --= rList
    }
    seenList.toList
  }

  def singleSeedFocused(graph: Graph[Int, Int], seed: Int, wave: Int): List[Int] = {
    val mList: EdgeRDD[Int] = graph.edges
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either)
    neighbors.foreach(a => {
      val str2 = a._2.toList.toString()
      println(a._1 + " " + str2);
    })
    var li: ListBuffer[(Int, Int)] = mList.map(e => (e.srcId.toInt, e.dstId.toInt)).collect().to[ListBuffer]
    val disc: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    val seenList: ListBuffer[Int] = new ListBuffer[Int]()
    seenList.append(seed)
    disc.add(seed)

    var w = 0;
    while (!li.isEmpty && w < wave) {
      val phase: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      w += 1
      val seenSet: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      var rList: mutable.Set[(Int, Int)] = new mutable.HashSet[(Int, Int)]()
      for ((a, b) <- li) {
        var f = false
        if (disc.contains(a)) {
          f = true
          if (disc.contains(b)) {
            seenList.append(b)
            seenList.append(a)
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
      seenList ++= seenSet
      disc ++= phase
      li --= rList
    }
    seenList.toList
  }
}
