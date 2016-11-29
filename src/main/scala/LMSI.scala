import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/25/2016.
  */
object LMSI {

  def main(args: Array[String]): Unit = {

  }

  def singleSeed(mList: mutable.Set[Tuple2[Int, Int]], seed:Int):List[Int]={

    val disc: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    var wave = 0;
    val seen: ListBuffer[Int] = new ListBuffer[Int]()
    seen.append(seed)
    disc.add(seed)

    while (mList.size != 0&&wave<=4) {
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
//      println("\n**"+wave + " " + phase.size + " " + disc.size + " " + mList.size + " Seen vertices " + seen.mkString(" "))
    }
    seen.toList


  }
}
