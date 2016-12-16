import java.io.FileWriter

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
    val grOptions = Map(("mu", 2.0), ("sigma", 1.3), ("vertices", 100))
    val fw: FileWriter = new FileWriter("waveApp3.txt");
    val header = "wave\tmu\tsigma\tvertices\tseedCount\tbootCount\tbootSamplePercentage\tnumVertices\tnumEdges\tmean\tavgGraphDeg\tvarianceOfBootStrapDegrees\n"
    fw.write(header);
    for (u <- 1 to 5) {
      val gr: Graph[Int, Int] = SyntheticData.synthGraphGenerator(sc, "lognormal", grOptions)
      val graph = GraphCleaning.cleanGraph(sc, gr)
      val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
      for (wave <- 1 to 4) {
        val expOptions: Map[String, Int] = Map(("seedCount", 10), ("bootCount", 50), ("wave", wave), ("bootSamplePercentage", 100), ("patchCount", 1))
        var t = System.currentTimeMillis()
        val txt = GraphBootPatchless.graphBoot(sc, graph, degrees, expOptions)
        fw.write(wave + "\t" + grOptions("mu") + "\t" + grOptions("sigma") + "\t" + grOptions("vertices") + "\t" + expOptions("seedCount") + "\t" + expOptions("bootCount") + "\t" + expOptions("bootSamplePercentage") + "\t" + txt + "\n")
        fw.flush()
      }
    }
    fw.close()
  }


}
