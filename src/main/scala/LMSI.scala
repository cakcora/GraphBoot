import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/25/2016.
  */
object LMSI {

  def singleSeed(mList: mutable.Set[Tuple2[Int, Int]], seed:Int, wave:Int):List[Int]={

    val disc: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    var w = 0;
    val seenList: ListBuffer[Int] = new ListBuffer[Int]()
    seenList.append(seed)
    disc.add(seed)
    while (mList.size != 0&&w<wave) {
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
      mList--=rList
    }
    seenList.toList
  }
}
