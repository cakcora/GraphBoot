import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by cxa123230 on 11/29/2016.
  */
object BootStrapper {
  def boot(bootCount: Int, proxySampleSize: Int, vertexList: List[Int], degrees: Map[Int, Int], seedSet: Set[Int]): List[Double] = {
    val bstrapDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val listLength: Int = vertexList.length
    for (i <- 1 to bootCount) {
      val kSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      val kNonSeedMap: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int].withDefaultValue(0)
      val random: Random = new Random()
      for (j <- 1 to proxySampleSize) {
        val chosen: Int = vertexList(random.nextInt(listLength))
        if (seedSet(chosen)) {
          kSeedMap(degrees(chosen)) += 1
        }
        else {
          kNonSeedMap(degrees(chosen)) += 1
        }
      }
      val numSeeds = kSeedMap.map(e => e._2.toInt).sum
      val numNonSeeds = kNonSeedMap.map(e => e._2.toInt).sum
      var avgDegree = 0.0
      val p0 = kSeedMap(0) / numSeeds
      for (i <- (kSeedMap ++ kNonSeedMap)) {
        val i1: Double = kSeedMap(i._1) + Math.abs(1 - p0) * kNonSeedMap(i._1)
        avgDegree += i._1 * i1 / ((numSeeds + numNonSeeds))
      }
      //add avg degree from this bootstrap
      bstrapDegrees += avgDegree
    }
    bstrapDegrees.toList
  }
}
