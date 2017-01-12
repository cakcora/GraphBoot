import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 11/15/2016.
  */
object UnifiedExperimentDriver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val fw: FileWriter = new FileWriter("exp4.txt");
    val header = "wave\tmu\tsigma\tvertices\tseedCount\tbootCount\tbootSamplePercentage\tnumVertices\tnumEdges\tmean\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tmuProxy\tl2\tlmin\tlmax\n"
    fw.write(header);
    val method = "par"
    for (cv <- 1 to 50) {
      println(" iter " + cv)
      for (sigma <- List(0.0, 0.2, 0.4, 0.8, 1.2, 1.4)) {
        val grOptions: Map[String, AnyVal] = Map(("mu", 2.0), ("sigma", sigma), ("vertices", 10000))

        val gr: Graph[Int, Int] = SyntheticData.synthGraphGenerator(sc, "lognormal", grOptions)
        val graph = GraphCleaning.cleanGraph(sc, gr)
        val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
        val h = 30
        val maxSeed = 30
        var max = 0;

        val proxymap = scala.collection.mutable.HashMap[String, Int]().withDefaultValue(0)
        val seeds: RDD[(VertexId, Int)] = Common.chooseSeeds(sc, graph, maxSeed)
        val muProxy: Double = proxyMu(graph, seeds, h)

        for (seed <- List(2, 5, 10, 20, maxSeed)) {
          for (wave <- 1 to 6) {
            val expOptions: Map[String, Int] = Map(("bootCount", 1000), ("wave", wave), ("bootSamplePercentage", 100), ("patchCount", 1))
            val txt = GraphBootPatchless.graphBoot(sc, graph, degrees, seeds.take(seed), expOptions, method)
            val l1: Double = txt("l1").asInstanceOf[Double]
            val l2: Double = txt("l2").asInstanceOf[Double]
            if (muProxy > l1 && muProxy < l2) {
              proxymap(seed + "_" + wave) += 1
              //              println("\t" + seed + " " + wave + ":" + proxymap(seed + "_" + wave))
              if (proxymap(seed + "_" + wave) > max) {
                max = proxymap(seed + "_" + wave)
              }
            }
            fw.write(wave + "\t" + grOptions("mu") + "\t" + grOptions("sigma") + "\t" + grOptions("vertices") + "\t" + seed + "\t" + expOptions("bootCount") + "\t" + expOptions("bootSamplePercentage") + "\t" + txt("vertices") + "\t" + txt("edges") + "\t" + txt("mean") + "\t" + txt("avgGraphDeg") + "\t" + txt("varianceOfBootStrapDegrees") + "\t" + txt("l1") + "\t" + muProxy + "\t" + txt("l2") + "\t" + txt("lmin") + "\t" + txt("lmax") + "\n")
            fw.flush()
          }
        }


      }
    }
    fw.close()
  }

  def proxyMu(graph: Graph[Int, Int], seeds: RDD[(VertexId, Int)], h: Int): Double = {
    var h2 = h
    if (seeds.count() < h) {
      println("proxy parameter " + h + " is too large")
      h2 = seeds.count().toInt
    }
    val proxyVertices: Array[VertexId] = seeds.takeSample(false, h2).map(e => (e._1))
    val map: RDD[Int] = graph.collectNeighbors(EdgeDirection.Either).filter(e => proxyVertices.contains(e._1)).map(e => e._2.length)

    return map.mean()
  }
}
