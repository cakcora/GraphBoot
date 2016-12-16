import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/25/2016.
  */
object LMSI {

  def singleSeed(edgeRDD: RDD[Edge[PartitionID]], seed: Int, wave: Int): List[Int] = {


    var edgeList: ListBuffer[(Int, Int)] = edgeRDD.map(e => (e.srcId.toInt, e.dstId.toInt)).collect().to[ListBuffer]

    val disc: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    val seenList: ListBuffer[Int] = new ListBuffer[Int]()
    seenList.append(seed)
    disc.add(seed)
    var w = 0;
    while (!edgeList.isEmpty && w < wave) {
      val phase: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      w += 1
      val seenSet: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      var rList: mutable.Set[(Int, Int)] = new mutable.HashSet[(Int, Int)]()
      for ((a, b) <- edgeList) {
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
      edgeList --= rList
    }
    seenList.toList
  }


}
