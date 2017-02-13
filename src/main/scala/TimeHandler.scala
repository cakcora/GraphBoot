import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

/**
  * Created by cxa123230 on 2/8/2017.
  * Experiment 3. Requires results from Holder.scala experiments
  */
object TimeHandler {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  val predResultFile: String = "results/classified/1resultPredsRestrictive.txt"


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
    val dataset = "time"

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
    val info0 = Source.fromFile(ClassifierData.infoFile).getLines().toList.map(f => f.split("\t"))
    println(info0.size + " users were found in the info file")
    val timed: Map[String, String] = info0.filter(f => f(10) != "null").map(e => (e(0), e(10))).toMap
    println(timed.size + " users with time")
    val times: Set[String] = timed.values.toSet
    println(times.size + " different timezones:" + times.mkString(", "))
    val userTimeCounts: mutable.Map[Int, Int] = getTimes(sc, depressedUsers, timed, idMap)

    println(userTimeCounts.size + " user profiles found.")

    val degreeMap: Map[Int, Int] = userTimeCounts.toMap

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
        val seedArray: Array[(VertexId, Int)] = seeds.map(e => (idMap(e), 0)).toArray
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

  def getTimes(sc: SparkContext, depUsers: Set[String], userTimes: Map[String, String], idMap: mutable.HashMap[String, VertexId]): mutable.Map[Int, Int] = {

    val depressedTweets: List[(String, String)] = Source.fromFile(ClassifierData.tweetFile).getLines().map(e => {
      val arr = e.split("\t")
      val user: String = arr(0)
      val tweetDate: String = arr(2)
      (user, tweetDate)
    }).toList.filter(f => depUsers.contains(f._1))
    //&&userTimes.contains(f._1)
    val buf = mutable.HashMap.empty[Int, Int].withDefaultValue(1)
    println(depressedTweets.length + " tweets from depressed users with a time zone.")

    for (row <- depressedTweets) {
      val usr = row._1
      val date = row._2
      if (date != "") {
        val twitterDate: Date = getTwitterDate(date)
        val tim = twitterDate.getHours
        if (tim < 6 || tim > 21) {
          buf(idMap(usr).toInt) += 1
        }
      }
    }
    buf
  }

  def getTwitterDate(date: String): Date = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss z yyyy",
      Locale.ENGLISH)
    return sdf.parse(date)
  }

}

