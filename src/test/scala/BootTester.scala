import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by cxa123230 on 1/14/2017.
  */
object BootTester {

  def main(args: Array[String]): Unit = {


    val ps: ArrayBuffer[(Int, Int)] = GraphBoot.reverseProbArray(List(1, 2, 3, 4), Map((1, 1), (2, 1), (3, 1), (4, 1)))

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
      val chosen: Int = GraphBoot.findInProbArray(probs, random.nextInt(inter))
      occMap(chosen) += 1
    }
    assert(occMap(2) > occMap(3))
    assert(occMap(4) > occMap(3))


    val pp: ArrayBuffer[(Int, Int)] = GraphBoot.reverseProbArray(List(1, 2, 3, 4), Map((1, 1), (2, 1), (3, 1000), (4, 1)))

    assert(2 == GraphBoot.findInProbArray(pp, 1002))
    assert(GraphBoot.findInProbArray(pp, 100) == 1)
    assert(GraphBoot.findInProbArray(pp, 1000) == 1)
    assert(GraphBoot.findInProbArray(pp, 1001) == 2)
    assert(GraphBoot.findInProbArray(pp, 1002) == 2)
    assert(GraphBoot.findInProbArray(pp, 2001) == 3)
    assert(GraphBoot.findInProbArray(pp, 2500) == 4)
    val p: ArrayBuffer[(Int, Int)] = GraphBoot.reverseProbArray(List(2, 3, 4), Map((1, 1), (2, 1), (3, 1000), (4, 1)))

    assert(GraphBoot.findInProbArray(p, 100) == 2)
    assert(GraphBoot.findInProbArray(p, 1000) == 2)
    assert(GraphBoot.findInProbArray(p, 1001) == 3)
    assert(GraphBoot.findInProbArray(p, 1002) == 4)
    assert(GraphBoot.findInProbArray(p, 2001) == 4)
    assert(GraphBoot.findInProbArray(p, 2500) == -1)


  }
}
