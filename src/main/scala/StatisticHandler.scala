import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

/**
  * Created by cxa123230 on 2/8/2017.
  * Experiment 3. Requires results from Holder.scala experiments
  */
object StatisticHandler {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  val predResultFile: String = "results/classified/1resultPredsRestrictive.txt"

  val symptomWords = List("anxiety", "withdrawal", "severe", "delusions", "adhd", "weight", "insomnia", "drowsiness", "suicidal", "appetite", "dizziness", "nausea", "episodes", "attacks", "sleep", "seizures", "addictive", "weaned", "swings", "dysfunction", "blurred", "irritability", "headache", "fatigue", "imbalance", "nervousness", "psychosis", "drowsy")
  val disclosureWords = List("fun", "play", "helped", "god", "answer", "wants", "leave", "beautiful", "suffer", "sorry", "tolerance", "agree", "hate", "helpful", "haha", "enjoy", "social", "talk", "save", "win", "care", "love", "like", "hold", "cope", "amazing", "discuss")
  val treatmentWords = List("medication", "side-effects", "doctor", "doses", "effective", "prescribed", "therapy", "inhibitor", "stimulant", "antidepressant", "patients", "neurotransmitters", "prescriptions", "psychotherapy", "diagnosis", "clinical", "pills", "chemical", "counteract", "toxicity", "hospitalization", "sedative", "150mg", "40mg", "drugs")
  val relationshipWords = List("home,", "woman,", "she,", "him,", "girl,", "game,", "men,", "friends,", "sexual,", "boy,", "someone,", "movie,", "favorite,", "jesus,", "house,", "music,", "religion,", "her,", "songs,", "party,", "bible,", "relationship,", "hell,", "young,", "style,", "church,", "lord,", "father,", "season,", "heaven,", "dating")
  val words = Map(("symptom", symptomWords), ("disclosure", disclosureWords), ("treatment", treatmentWords), ("relationship", relationshipWords))

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
    val dataset = "treatment"
    //"treatment","disclosure","symptom","relationship"
    if (!words.contains(dataset)) System.exit(1)
    val usedWords = words(dataset)
    println(seeds.size + " seeds are used.")


    val dp: Set[String] = Source.fromFile(predResultFile).getLines().map(e => e.split("\t")).filter(f => f(1) == "1.0").map(e => e(0)).toSet
    val depressedUsers: Set[String] = dp.union(seeds)
    println(depressedUsers.size + " depressed users' names were found.")
    val edgeList: List[(String, String)] = Source.fromFile(ClassifierData.infoFile).getLines().toList.map(e => {
      val arr = e.split("\t")
      (arr(0), arr(1))
    })
    val edges: Set[(String, String)] = edgeList.filter(e => depressedUsers.contains(e._1) && depressedUsers.contains(e._2)).toSet
    println(edges.size + " edges have been found among depressed users.")
    val idMap: mutable.HashMap[String, VertexId] = mutable.HashMap.empty[String, Long]
    var ind = 0;
    for (s <- depressedUsers) {
      idMap(s) = ind
      ind += 1
    }

    val edgeSet: Array[(Long, Long)] = edges.map(e => (idMap(e._1), idMap(e._2))).toArray
    println(edgeSet.length + " edges are prepared for the graph")
    val returnRawProfiles: Boolean = true
    val userProfiles: Map[String, String] = Holder.getProfiles(sc, depressedUsers, returnRawProfiles).filter(f => depressedUsers.contains(f._1)).collect.toMap

    println(userProfiles.size + " user profiles found.")
    val buf: ArrayBuffer[(PartitionID, PartitionID)] = mutable.ArrayBuffer.empty[(Int, Int)]
    for (user <- depressedUsers) {
      var deg = 1
      if (userProfiles.contains(user)) {
        val profile = userProfiles(user).replaceAll("[^A-Za-z0-9 ]", " ").toLowerCase()
        val text = profile.split(" ").filter(f => f.length > 1)
        for (t <- text) {
          if (usedWords.contains(t)) deg += 1
        }
      }
      buf.append((idMap(user).toInt, deg))
    }
    val degreeMap: Map[Int, Int] = buf.toMap

    println(degreeMap.size + " users degrees found")
    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(sc.makeRDD(edgeSet), defaultValue = 0)
    println(graph.numVertices + " nodes, " + graph.numEdges + " edges")
    val g2 = GraphCleaning.cleanGraph(sc, graph)
    println(g2.numVertices + " nodes, " + g2.numEdges + " edges")

    val fw: FileWriter = new FileWriter("exp" + dataset + ".txt");
    val header = "method\twave\tlmsiAll\tlmsiDistinct\tmean\tmedGraphDeg\tavgGraphDeg\tvarianceOfBootStrapDegrees\tl1\tl2\tlmin\tlmax\n"
    fw.write(header);
    for (q <- (1 to 50 by 1)) {
      println("Iteration: " + q)
      for (wave <- List(0, 1, 2)) {
        val expOptions: Map[String, Int] = Map(("bootCount", 1000), ("wave", wave))
        val seedArray: Array[(VertexId, Int)] = Random.shuffle(seeds.toList).take(5).map(e => (idMap(e), 0)).toArray
        val txtPar = GraphBoot.compute(sc, g2, degreeMap, seedArray, expOptions, "parSpark")
        printResults(fw, expOptions, txtPar, "parSpark")
      }
    }
    fw.close
  }

  def printResults(fw: FileWriter, expOptions: Map[String, Int], txt: Map[String, AnyVal], method: String): Unit = {
    fw.write(method + "\t" + expOptions("wave") + "\t" + txt("lmsiAll") + "\t" + txt("lmsiDistinct") + "\t" + txt("mean") + "\t" + txt("medGraphDeg") + "\t" + txt("avgGraphDeg") + "\t" + txt("varianceOfBootStrapDegrees") + "\t" + txt("l1") + "\t" + txt("l2") + "\t" + txt("lmin") + "\t" + txt("lmax") + "\n")
    fw.flush()
  }
}

