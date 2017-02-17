
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by cxa123230 on 2/13/2017.
  * requires results from holder.scala.
  */
object ValidationHandler {
  def main(args: Array[String]): Unit = {
    val people = List("cuneyt", "ceren", "asim", "aref")
    val root: String = "C:/Users/cxa123230/IdeaProjects/GraphBoot/results/"
    val dataDir = root + "data/"
    val predResultFile = root + "classified/0resultPredsAll.txt"
    val labelsFromPredictions: Map[String, String] = Source.fromFile(predResultFile).getLines().map(e => e.split("\t")).map(e => (e(0), e(1))).toMap

    val usrMap = mutable.HashMap.empty[String, Map[String, Int]]
    for (usr <- people) {
      val f: Map[String, Int] = Source.fromFile(dataDir + "manual" + usr + ".txt").getLines().toList.map(e => e.split("\t")).map(f => if (f.length == 2) (f(0), f(1).toInt) else (f(0), 2)).toMap
      usrMap(usr) = f
    }

    val L: Int = 171
    val iArray = ArrayBuffer.fill(L)(0.0)
    val jArray = ArrayBuffer.fill(3)(0.0)
    val P: Int = people.size
    val N = P * L
    var index = 0
    val trueArray = ArrayBuffer.fill(2)(0)
    val falseArray = ArrayBuffer.fill(2)(0)
    for (row <- labelsFromPredictions) {
      var outputString = ""
      val twitterUser = row._1
      val arr = ArrayBuffer.fill(4)(0)
      val lbl = row._2.toDouble.toInt
      var f = false
      for (labeler <- people) {
        if (usrMap(labeler).contains(twitterUser)) {
          val mapVal: Int = usrMap(labeler)(twitterUser)
          arr(mapVal) += 1
          outputString += (mapVal + "\t")
          f = true
        }
        else {
          outputString += "\t"
        }
      }
      if (f) {
        println(twitterUser + "\t" + lbl + "\t" + outputString)
        iArray(index) = (1.0 / (P * (P - 1.0))) * (arr(0) * arr(0) + arr(1) * arr(1) + arr(2) * arr(2) + arr(3) * arr(3) - P)
        jArray(0) = arr(0).toDouble / L
        jArray(1) = arr(1).toDouble / L
        jArray(2) = arr(2).toDouble / L
        if (lbl == 0) {
          if (arr(0) > arr(1)) {
            //true negative
            trueArray(0) += 1
          }
          if (arr(0) < arr(1)) {
            //false negative
            falseArray(0) += 1
          }
        }
        else if (lbl == 1) {
          if (arr(1) > arr(0)) {
            //true positive
            trueArray(1) += 1
          }
          if (arr(0) > arr(1)) {
            //false positive
            falseArray(1) += 1
          }
        }

        index += 1
      }
    }
    println(index + " values (make sure that L is set to this value in the code)")
    val iHat = iArray.sum / index
    val eHat = jArray.map(e => (e) * (e)).sum


    val kappa = (iHat - eHat) / (1.0 - eHat)

    println(kappa + " between " + P + " annotators")

    println("false positive: " + falseArray(1))
    println("false negative: " + falseArray(0))
    println("true positive: " + trueArray(1))
    println("true negative: " + trueArray(0))
  }


}
