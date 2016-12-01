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
      .appName("sd")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val options = Map(("mu", 2.0), ("sigma", 1.3), ("vertices", 5))
    val graph: Graph[PartitionID, PartitionID] =  Graph.fromEdges(sc.parallelize(Array(Edge(0,4,1), Edge(0,3,1), Edge(1,3,1), Edge(0,2,1), Edge(0,1,1),Edge(1,2,1), Edge(3,4,1), Edge(1,4,1))),defaultValue = 1)
    println(graph.edges.collect().mkString(" "))
    val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    for (wave <- 1 to 4 by 1) {
      val txt = GraphBootApproach2.graphBoot(100, graph, degrees, sc, 100, 3, wave, 10)
      println(wave+"\t"+2 + "\t" + 1.3 + "\t" + 5 + "\t" + 100 + "\t" + 10 + "\t" + 3 + "\t" + 100 + "\t" + txt + "\n")

    }
  }


}
