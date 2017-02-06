import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by cxa123230 on 2/4/2017.
  */
object ClassifierData {
  val minWordLength = 3
  val minWordCount = 3
  val minTopicCount = 3
  val fwFile = "featureWords.txt"
  val outFile = "inputGB0.txt"
  val infoFile = "userinfo.txt"
  val tweetFile = "timeline.txt"
  val stopFile = "terrier-stop.txt"
  val predResultFile: String = "resultPreds.txt"
  val seeds: Set[String] = Source.fromFile("seedList.txt").getLines().toSet
  def getMinWordLength(): Int = return minWordLength

  def getMinWordCount(): Int = return minWordCount

  def getMinTopicCount(): Int = return minTopicCount

  def getSeeds(): Set[String] = return seeds

  def getFwFile(): String = return fwFile

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sc = spark.sparkContext
    import spark.implicits._


    val stops = spark.read.textFile(stopFile).map(r => r.toString).collect().toSet
    println(stops.size + " stop words.")

    val sf = Source.fromFile(tweetFile).getLines().map(e => e.split("\t")).filter(e => seeds.contains(e(0))).toList
    val nsf = Source.fromFile("nseedTweets.txt").getLines().map(e => e.split("\t")).toList

    val stweets: RDD[(String, String)] = sc.makeRDD(sf).map(e => (e(0), clean(e(12), stops, minWordLength))).reduceByKey(_ + " " + _)
    val n = (100 / 36) * stweets.count().toInt
    if (false) {
      createNSeedTweets(sc, stops, minWordLength)
    }
    println("labeled1: " + stweets.count() + " labeled0:" + n)
    val nsTweets: RDD[(String, String)] = sc.makeRDD(sc.makeRDD(nsf).takeSample(false, n).map(f => (f(0), clean(f(1), stops, minWordLength)))).reduceByKey(_ + " " + _)

    val swords = stweets.union(nsTweets).flatMap(f => f._2.split(" ")).map(e => (e, 1)).reduceByKey(_ + _)
    val wordIds = convert2Id(minWordCount, swords)

    new FileWriter(fwFile).append(wordIds.map(e => e._1 + ":" + e._2).toList.mkString("\r\n")).close()

    val seedTweets: ListBuffer[String] = orderTweets(sc, stweets, wordIds, minTopicCount)
    val nonSeedTweets: ListBuffer[String] = orderTweets(sc, nsTweets, wordIds, minTopicCount)


    val s1: RDD[(Int, String)] = sc.makeRDD(seedTweets.map(s => (1, s)))
    val s2: RDD[(Int, String)] = sc.makeRDD(nonSeedTweets.map(s => (0, s)))
    val allTweets = s1.union(s2)

    val fw = new FileWriter(outFile)
    allTweets.collect().map(e => fw.append(e._1 + " " + e._2 + "\r\n"))
    fw.close();

  }

  def orderTweets(sc: SparkContext, stweets: RDD[(String, String)], wordIds: mutable.HashMap[String, Int], minTopicCount: Int): ListBuffer[String] = {
    val inS: RDD[(String, String)] = stweets.map(e => (e._1, e._2.toString.split(" ").filter(s => wordIds.contains(s)).sorted.mkString(" ")))
    val inSS = scala.collection.mutable.ListBuffer.empty[(String)]

    for (a <- inS.collect()) {
      val tweetWords: String = a._2
      val collect: Array[(String, Int)] = sc.makeRDD(tweetWords.split(" ")).map(e => (e, 1)).reduceByKey(_ + _).collect()
      val counts = collect.map(e => (wordIds(e._1), e._2))
      if (tweetWords.split(" ").length > minTopicCount)
        inSS.append((counts.sorted.map(e => e._1 + ":" + e._2).mkString(" ")))
    }
    inSS
  }

  def createNSeedTweets(sc: SparkContext, stops: Set[String], wordLength: Int): Unit = {
    var nsf = Source.fromFile("C:\\Projects\\DisagioData\\training_set_tweets.txt").getLines()
    val ma = scala.collection.mutable.HashMap.empty[String, String].withDefaultValue("")

    val nseedF = nsf.map(e => e.split("\t")).filter(e => e.length == 4).map(e => (e(0), clean(e(2), stops, wordLength))).toList
    val ntweets = sc.makeRDD(nseedF).reduceByKey(_ + " " + _).takeSample(false, 40000)

    val fw = new FileWriter("nseedTweets.txt")
    ntweets.foreach(f => fw.append(f._1 + "\t" + f._2 + "\r\n"))
    fw.close()
  }


  def convert2Id(minWordCount: Int, swords: RDD[(String, Int)]): mutable.HashMap[String, Int] = {
    val wordIds = mutable.HashMap.empty[String, Int]
    val wordList = swords.filter(e => {
      e._2 > minWordCount
    }).map(e => e._1).distinct().collect().toSet
    var ind = 100;
    for (x <- wordList.toList.sorted) {
      wordIds(x) = ind
      ind += 1
    }
    return wordIds
  }

  def clean(tweet: String, stops: Set[String], wordLength: Int): String = {

    if (tweet == null) return " "
    val all: String = tweet.toString.toLowerCase().replaceAll("[^A-Za-z0-9]", " ")
    val li = all.split(" ").filter(e => (e.length > wordLength && !stops.contains(e.trim)))
    if (li == null) return " "
    return li.mkString(" ");
  }


}