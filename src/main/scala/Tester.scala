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
    val options = Map(("mu", 2.0), ("sigma", 1.3), ("vertices", 1000))
    val gr: Graph[PartitionID, PartitionID] = SyntheticData.synthGraphGenerator(sc, "lognormal", options)
    val graph = GraphCleaning.cleanGraph(sc, gr)
    val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap

    for (wave <- 1 to 4) {
      var t = System.currentTimeMillis()
      val txt = GraphBootApproach2.graphBootAllSubGraph(px = 100, graph, degrees, sc, sx = 10, patchCount = 3, wave, bootCount = 10)
      println(wave + "\t" + 2 + "\t" + 1.3 + "\t" + 5 + "\t" + 100 + "\t" + 10 + "\t" + 3 + "\t" + 100 + "\t" + txt + "\n")
      println(System.currentTimeMillis() - t)
    }

  }


}
