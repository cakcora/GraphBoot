import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/25/2016.
  */
object LMSI {

  def singleSeed(mList: RDD[(Int, Int)], seed: Int, wave: Int): List[Int] = {
    var li: ListBuffer[(Int, Int)] = mList.collect().to[ListBuffer]
    val disc: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    var w = 0;
    val seenList: ListBuffer[Int] = new ListBuffer[Int]()
    seenList.append(seed)
    disc.add(seed)
    while (li.size != 0 && w < wave) {
      val phase: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      w += 1
      val seenSet: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      var rList: mutable.Set[Tuple2[Int, Int]] = new mutable.HashSet[Tuple2[Int, Int]]()
      for ((a, b) <- mList) {
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
}
