import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable

/**
  * Created by cxa123230 on 1/14/2017.
  */
object BootTester {

  def main(args: Array[String]): Unit = {


    val p1: (mutable.LinkedHashMap[Int, Int], Int) = GraphBoot.reverseProbMap(List(1, 2, 3, 4), Map((1, 1), (2, 1), (3, 1), (4, 1)))

    var probs = p1._1
    assert((probs(2) - probs(1)) == probs(1))
    assert(p1._2 == 4000)
    val p2: (mutable.LinkedHashMap[Int, Int], Int) = GraphBoot.reverseProbMap(List(1, 2, 3, 4), Map((1, 1), (2, 1), (3, 1000), (4, 1)))
    probs = p2._1
    assert((probs(2) - probs(1)) == probs(1))
    assert(p2._2 == 3001)

    val p3: (mutable.LinkedHashMap[Int, Int], Int) = GraphBoot.reverseProbMap(List(2, 3, 4), Map((1, 1), (2, 1), (3, 1000), (4, 1)))
    probs = p3._1
    assert(probs(2) == (probs(4) - probs(3)))
    val random: ThreadLocalRandom = ThreadLocalRandom.current()
    val occMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
    for (x <- 1 to 10000) {
      val chosen: Int = GraphBoot.pickWithProb(probs, random.nextInt(p3._2))
      occMap(chosen) += 1

    }
    assert(occMap(2) > occMap(3))
    assert(occMap(4) > occMap(3))
    val occMap2: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
    for (x <- 1 to 10000) {
      val chosen: Int = GraphBoot.pickWithProb(p1._1, random.nextInt(p1._2))
      occMap2(chosen) += 1

    }
    assert(occMap2(2) > p1._2 / 5)
    assert(occMap2(3) > p1._2 / 5)
    assert(occMap2(4) > p1._2 / 5)
    assert(occMap2(1) > p1._2 / 5)
  }
}
