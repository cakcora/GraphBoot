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
    val seeds = List("CDepression_UK", "AlexElk123", "ImNotFine_x", "ChildMindDotOrg", "BipolarTu", "worthlivingnow", "natasha_tracy", "CPMHealthCare", "BipolarGrrl", "sadinthehead", "PROJECT375", "angelsdemonsorg", "m_vanackeren")

  }

}
