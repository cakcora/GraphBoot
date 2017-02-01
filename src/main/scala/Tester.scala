import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 1/23/2017.
  */
object Tester {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()

    val sc = spark.sparkContext
    var count = 0;
    val rate = 2;
    var nextTime = System.currentTimeMillis() + rate * 60 * 1000
    for (seed <- List(1, 2, 3, 4, 5, 6, 7, 8, 9)) {
      count = (count + 1) % (rate + 1)
      if (count == 0) {
        while (nextTime > System.currentTimeMillis()) {
          val waitTime: Long = 10 * 1000
          Thread.sleep(waitTime)
        }
        nextTime = System.currentTimeMillis() + rate * 60 * 1000
        count = 1
      }
      println("Using resource " + count + "for " + seed + " at time " + System.currentTimeMillis() / 1000)

    }


  }

}
