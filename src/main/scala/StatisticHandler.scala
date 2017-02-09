import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

/**
  * Created by cxa123230 on 2/8/2017.
  * Experiment 3. Requires results from Holder experiments
  */
object StatisticHandler {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    val seeds: Set[String] = ClassifierData.getSeeds()
    println(seeds.size + " seeds are used.")
    val symptomWords = List("anxiety", "withdrawal", "severe", "delusions", "adhd", "weight", "insomnia", "drowsiness", "suicidal", "appetite", "dizziness", "nausea", "episodes", "attacks", "sleep", "seizures", "addictive", "weaned", "swings", "dysfunction", "blurred", "irritability", "headache", "fatigue", "imbalance", "nervousness", "psychosis", "drowsy")

    val depUsers: Set[String] = Source.fromFile(ClassifierData.predResultFile).getLines().map(e => e.split("\t")).filter(f => f(1) == "1.0").map(e => e(0)).toSet
    println(depUsers.size + " depressed users were found.")
    val dp = depUsers.union(seeds)
    val edges = Source.fromFile(ClassifierData.infoFile).getLines().map(e => {
      val arr = e.split("\t")
      (arr(0), arr(1))
    }).toSet.filter(e => dp.contains(e._1) && dp.contains(e._2))
    val idMap = mutable.HashMap.empty[String, Long]
    var ind = 0;
    for (s <- dp) {
      idMap(s) = ind
      ind += 1
    }
    val seedArray: Array[(VertexId, Int)] = seeds.map(e => (idMap(e), 0)).toArray
    val edgeSet: Array[(Long, Long)] = edges.map(e => (idMap(e._1), idMap(e._2))).toArray
    val userProfiles: Map[String, String] = Holder.getProfiles(sc, dp).filter(f => depUsers.contains(f._1)).collect.toMap
    println(userProfiles.size)
    val buf = mutable.ArrayBuffer.empty[(Int, Int)]
    for (user <- depUsers) {
      var deg = 0
      if (userProfiles.contains(user)) {
        val profile = userProfiles(user)
        val text: Map[String, Int] = profile.split(" ").map(e => {
          val arr = e.split(":")
          (arr(0), arr(1).toInt)
        }).toMap

        for (t <- text) {
          if (symptomWords.contains(t._1)) deg += symptomWords(t._2).toInt
        }
      }
      buf.append((idMap(user).toInt, deg))
    }
    val degreeMap: Map[Int, Int] = buf.toMap
    println(degreeMap.size + " users degrees found")
    val graph = Graph.fromEdgeTuples(sc.makeRDD(edgeSet), defaultValue = 0)
    println(graph.numVertices + " nodes, " + graph.numEdges + " edges")
    val g2 = GraphCleaning.cleanGraph(sc, graph)
    println(g2.numVertices + " nodes, " + g2.numEdges + " edges")
    val expOptions: Map[String, Int] = Map(("bootCount", 1000), ("wave", 2))
    val txtPar = GraphBoot.compute(sc, graph, degreeMap, seedArray, expOptions, "parSpark")
    val fw: FileWriter = new FileWriter("exp" + "symptoms" + ".txt");
    val header = "method\twave\tseed\tlmsiAll\tlmsiDistinct\tmean\tmedGraphDeg\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tmuProxy\tl2\tlmin\tlmax\n"
    fw.write(header);
    printResults(fw, 2, expOptions, txtPar, "parSpark")
  }

  def printResults(fw: FileWriter, wave: Int, expOptions: Map[String, Int], txt: Map[String, AnyVal], method: String): Unit = {
    fw.write(method + "\t" + wave + "\t" + txt("lmsiAll") + "\t" + txt("lmsiDistinct") + "\t" + txt("mean") + "\t" + txt("medGraphDeg") + "\t" + txt("avgGraphDeg") + "\t" + txt("varianceOfBootStrapDegrees") + "\t" + txt("l1") + "\t" + txt("l2") + "\t" + txt("lmin") + "\t" + txt("lmax") + "\n")
  }
}

