/**
  * Created by cxa123230 on 1/17/2017.
  */
object CommonTester {

  def main(args: Array[String]): Unit = {
    var mean = Common.proxyMu(Array(1, 2, 3, 4), Map((1, 4), (2, 5), (3, 3), (4, 4)))
    assert(mean == 4)
    mean = Common.proxyMu(Array(1, 2, 3, 4), Map((1, 0), (2, 5), (3, 3), (4, 4)))
    assert(mean == 3)
  }
}
