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
  * requires an initial set of seeds, and external tweets.
  */
object ClassifierData {
  val minWordLength = 3
  val minWordCount = 3
  val minTopicCount = 3
  val fwFile = "featureWords.txt"
  val outFile = "inputGB.txt"
  val infoFile = "userinfo.txt"
  val tweetFile = "timeline.txt"
  val stopFile = "results/data/terrier-stop.txt"
  val labelFile: String = "results/data/agreedlabels.txt"
  val predResultFile: String = "resultPreds.txt  "
  val seeds: Set[String] = Source.fromFile("results/data/seedList.txt").getLines().toSet

  def getMinWordLength(): Int = minWordLength

  def getMinWordCount(): Int = minWordCount

  def getMinTopicCount(): Int = minTopicCount

  def getSeeds(): Set[String] = seeds

  def getFwFile(): String = fwFile

  def cleanAll(nsf: List[Array[String]], stops: Set[String]): List[(String, String)] = {
    val buf = mutable.ArrayBuffer.empty[(String, String)]
    for (row <- nsf) {
      buf.append((row(0), clean(row(1), stops, minWordLength)))
    }
    return buf.toList
  }

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

    val stops: Set[String] = spark.read.textFile(stopFile).map(r => r.toString).collect().toSet
    println(stops.size + " stop words.")

    val allLabeled: Iterator[(String, Int)] = Source.fromFile(labelFile).getLines().map(e => {
      val arr = e.toString().split("\t")
      (arr(0), arr(1).toInt)
    })
    val depLabeledUsers: Map[String, Int] = allLabeled.filter(f => f._2 == 1).toMap[String, Int]
    val nonDepLabeledUsers: Map[String, Int] = allLabeled.filter(f => f._2 == 0).toMap[String, Int]
    val depUsers = seeds.union(depLabeledUsers.keySet)
    val nonDepUsers = seeds.union(nonDepLabeledUsers.keySet)
    val seedDepTweets = Source.fromFile(tweetFile).getLines().map(e => e.split("\t")).filter(e => depUsers.contains(e(0))).toList
    val seedNonDepTweets = Source.fromFile(tweetFile).getLines().map(e => e.split("\t")).filter(e => nonDepUsers.contains(e(0))).toList



    val seedDepUsers: RDD[(String, String)] = sc.makeRDD(seedDepTweets).map(e => (e(0), clean(e(12), stops, minWordLength))).reduceByKey(_ + " " + _)
    val seedNonDepUsers: RDD[(String, String)] = sc.makeRDD(seedNonDepTweets).map(e => (e(0), clean(e(12), stops, minWordLength))).reduceByKey(_ + " " + _)
    val percOfLabeledData: Int = 36
    val n = ((100 / percOfLabeledData) * (seedNonDepUsers.count() + seedDepUsers.count())).toInt
    if (false) {
      //external tweet profiles are create from the cikm 2010 papers,
      //they are prepared once, and used in the experiments.
      createNSeedTweets(sc, stops, minWordLength)
    }
    println("seed depressed: " + seedDepUsers.count() + " seed non depressed :" + seedNonDepUsers.count())
    val nsf: List[Array[String]] = Source.fromFile("nseedTweets.txt").getLines().toList.map(e => e.split("\t"))
    val extProfiles: List[(String, String)] = nsf.filter(f => f.length == 2).map(e => (e(0), e(1)))
    val nsTweets: List[(String, String)] = sc.makeRDD(extProfiles).reduceByKey(_ + " " + _).takeSample(false, n).toList
    println("external non depressed " + nsTweets.size)

    val swords = seedDepUsers.union(sc.makeRDD(nsTweets)).union(seedNonDepUsers).flatMap(f => f._2.split(" ")).map(e => (e, 1)).reduceByKey(_ + _)


    val wordIds: mutable.HashMap[String, Int] = convert2Id(minWordCount, swords)

    new FileWriter(fwFile).append(wordIds.map(e => e._1 + ":" + e._2).toList.mkString("\r\n")).close()

    val seedTweetsDep: ListBuffer[String] = orderTweets(sc, seedDepUsers, wordIds, minTopicCount)
    val seedTweetsNon: ListBuffer[String] = orderTweets(sc, seedNonDepUsers, wordIds, minTopicCount)
    val nonSeedTweets: ListBuffer[String] = orderTweets(sc, sc.makeRDD(nsTweets), wordIds, minTopicCount)


    val s1: RDD[(Int, String)] = sc.makeRDD(seedTweetsDep.map(s => (1, s)))
    val s1_2: RDD[(Int, String)] = sc.makeRDD(seedTweetsNon.map(s => (0, s)))
    val s2: RDD[(Int, String)] = sc.makeRDD(nonSeedTweets.map(s => (0, s)))
    val allTweets = s1.union(s2).union(s1_2)

    val fw = new FileWriter(outFile)
    allTweets.collect().map(e => fw.append(e._1 + " " + e._2 + "\r\n"))
    fw.close()

  }

  def orderTweets(sc: SparkContext, stweets: RDD[(String, String)], wordIds: mutable.HashMap[String, Int], minTopicCount: Int): ListBuffer[String] = {
    val inS: RDD[(String, String)] = stweets.map(e => (e._1, e._2.toString.split(" ").filter(s => wordIds.contains(s)).sorted.mkString(" ")))
    val inSS = scala.collection.mutable.ListBuffer.empty[(String)]

    for (a <- inS.collect()) {
      val tweetWords: String = a._2
      val collect: Array[(String, Int)] = sc.makeRDD(tweetWords.split(" ")).map(e => (e, 1)).reduceByKey(_ + _).collect()
      val counts = collect.map(e => (wordIds(e._1), e._2))
      if (tweetWords.split(" ").length > minTopicCount)
        inSS.append(counts.sorted.map(e => e._1 + ":" + e._2).mkString(" "))
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
    }).map(e => e._1).distinct().filter(f => f.length > minWordLength).collect().toSet
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