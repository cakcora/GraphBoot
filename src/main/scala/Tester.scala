import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
  */
object Tester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext


    val grOptions: Map[String, AnyVal] = Map(("mu", 2.0), ("sigma", 0.0), ("vertices", 1000))
    val graph: Graph[Int, Int] = SyntheticData.synthGraphGenerator(sc, "lognormal", grOptions)
    println(graph.numVertices + " " + graph.numEdges)


  }

}
