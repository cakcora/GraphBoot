import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by cxa123230 on 1/14/2017.
  */
object BootTester {

  def main(args: Array[String]): Unit = {


    val p1: ArrayBuffer[(Int, Int)] = GraphBoot.reverseProbArray(List(1, 2, 3, 4), Map((1, 1), (2, 1), (3, 1), (4, 1)))

    var ps = p1
    assert((ps(1)._2 - ps(0)._2) == ps(0)._2)
    assert(ps(3)._2 == 4000)

    val p2: ArrayBuffer[(Int, Int)] = GraphBoot.reverseProbArray(List(1, 2, 3, 4), Map((1, 1), (2, 1), (3, 1000), (4, 1)))
    var probs = p2
    assert((probs(1)._2 - probs(0)._2) == probs(0)._2)
    assert(p2(3)._2 == 3001)

    val p3: ArrayBuffer[(Int, Int)] = GraphBoot.reverseProbArray(List(2, 3, 4), Map((1, 1), (2, 1), (3, 1000), (4, 1)))
    probs = p3
    assert(probs(0)._2 == (probs(2)._2 - probs(1)._2))
    val random: ThreadLocalRandom = ThreadLocalRandom.current()
    val occMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
    val inter = probs(probs.size - 1)._2
    for (x <- 1 to 10000) {
      val chosen: Int = GraphBoot.pickWithProbArray(probs, random.nextInt(inter))
      occMap(chosen) += 1
      //
    }
    println(probs + " " + occMap)
    assert(occMap(2) > occMap(3))
    assert(occMap(4) > occMap(3))

  }
}
