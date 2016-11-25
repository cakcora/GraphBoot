import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/15/2016.
  */
object Tester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("sd")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    for(e<-1 to 6){
    val g = GraphCleaning.cleanGraph(sc,SyntheticData.synthGraphGenerator(sc,"lognormal"))
      val avgGraphDeg: Double = breeze.stats.mean(g.degrees.map(_._2.toDouble).collect())
    println(g.numVertices+" "+g.numEdges+ " "+avgGraphDeg)
  }

  }


}
