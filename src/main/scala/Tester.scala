import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 1/12/2017.
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

    var graph: Graph[Int, Int] = SyntheticData.synthGraphGenerator(sc, "dblp", Map.empty)
    val seeds: RDD[(VertexId, Int)] = sc.makeRDD(graph.vertices.takeSample(false, 5).map(e => (e._1, 0)))
    println(seeds.collect().toList)

    val wave = 2
    graph = GraphCleaning.cleanGraph(sc, graph)

    val l1 = LMSI.parallelLMSISparkified(graph, seeds, wave)

    val l2 = LMSI.parallelLMSI(graph, seeds, wave)

    println(l1.diff(l2).size)
  }

}
