import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by cxa123230 on 1/25/2017.
  */
object HoldSampleFramework {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val networkName = "twitter"
    val fw: FileWriter = new FileWriter("exp" + networkName + ".txt");
    val header = "wave\tvertices\tseed\tbootCount\tlmsiAll\tlmsiDistinct\tmean\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tl2\tlmin\tlmax\n"
    fw.write(header);

    val dir = "C:\\Projects\\DisagioData\\"
    val fileName = "twitter_rv.net"
    val wave = 2
    val fr = Source.fromFile(dir + fileName).getLines()

    /*seed 1 cuneytgurcan 38813037

    @iFredorg
    @NateAgapi
    @onlimelite
    @natasha_tracy
    @AlexElk123
    */
    /*
    38813037	21467265
38813037	35603047
38813037	44703736
38813037	46013195
38813037	46870841
38813037	48888731
38813037	50298574
38813037	52686346
     */
    for (f <- fr) {
      val arr = f.split("\t")
      val v1 = arr(0).toInt
      val v2 = arr(1).toInt
      if (v1 == 171367366 || v2 == 171367366) println(f)
    }

  }
}
