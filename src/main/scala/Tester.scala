import scala.collection.mutable.ArrayBuffer

/**
  * Created by cxa123230 on 1/12/2017.
  */
object Tester {

  def main(args: Array[String]): Unit = {


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
