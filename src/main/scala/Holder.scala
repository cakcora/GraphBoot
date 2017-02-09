import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by cxa123230 on 2/2/2017.
  */
object Holder {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  var testLabel: Int = 2


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
    val nextUsers: Set[String] = getFriendsOf(seeds).filter(e => !seeds.contains(e))
    val nextNextUsers: Set[String] = getFriendsOf(nextUsers)
    val wave1Profiles: Array[(String, String)] = getProfiles(sc, nextNextUsers).collect()
    val inputFileForClasification: String = "inputTemp.txt"
    val wave0Profiles = Source.fromFile(ClassifierData.outFile).getLines()
    val inputFileHandle = new FileWriter(inputFileForClasification);
    for (n <- wave0Profiles) {
      inputFileHandle.append(n + "\r\n");
    }
    val idMap = mutable.HashMap.empty[Int, String]
    var assignedId = 10 //ids start from 10
    for (userProfile <- wave1Profiles) {
      inputFileHandle.append(assignedId + " " + userProfile._2 + "\r\n");
      idMap(assignedId) = userProfile._1
      assignedId += 1
    }
    inputFileHandle.close();
    val data = spark.read.format("libsvm").load(inputFileForClasification)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)
    val trainingData: Dataset[Row] = data.filter(e => e.get(0) == 1 || e.get(0) == 0)
    val testData: Dataset[Row] = data.filter(e => e.get(0) != 1 && e.get(0) != 0)
    println("Training on " + trainingData.count() + " testing on :" + testData.count())
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val model = pipeline.fit(trainingData)
    model.explainParams()
    println("Learned a model.")
    // Make predictions.
    val predictions = model.transform(testData)
    val ow = new FileWriter(ClassifierData.predResultFile)
    savePredictionsToFile(idMap, predictions, ow)
  }

  def savePredictionsToFile(idMap: mutable.HashMap[Int, String], predictions: DataFrame, ow: FileWriter): Unit = {
    val preds: Array[Row] = predictions.select("label", "predictedLabel").rdd.collect()
    for (row <- preds) {
      val lb = idMap(row.getAs[Double]("label").toInt)
      val pl = row.getAs[String]("predictedLabel")

      ow.append(lb + "\t" + pl + "\r\n")
    }
    ow.close
  }

  def getProfiles(sc: SparkContext, nextUsers: Set[String]): RDD[(String, String)] = {

    val tweetFile: List[(String, String)] = Source.fromFile(ClassifierData.tweetFile).getLines().map(e => {
      val arr = e.split("\t")
      (arr(0), arr(12))
    }).toList
    val nextTweets: List[(String, String)] = tweetFile.filter(e => nextUsers.contains(e._1)).map(e => (e._1, e._2))
    val stops: Set[String] = Source.fromFile(ClassifierData.stopFile).getLines().map(r => r.toString).toSet
    val tweets: Array[(String, String)] = sc.makeRDD(nextTweets).map(e => (e._1, ClassifierData.clean(e._2, stops, ClassifierData.getMinWordLength()))).reduceByKey(_ + " " + _).collect()
    println(tweets.length + " users' tweets have been found.")
    val wordIds = Source.fromFile(ClassifierData.fwFile).getLines().map(e => e.split(":")).map(e => (e(0), e(1).toInt)).toMap
    val buf = scala.collection.mutable.ListBuffer.empty[(String, String)]
    var ignored = 0
    for (row <- tweets) {
      val usr = row._1
      val split: Array[String] = row._2.split(" ")


      val hm: mutable.Map[Int, Int] = mutable.HashMap.empty[Int, Int].withDefaultValue(0)
      for (s <- split) {
        if (wordIds.contains(s)) {
          hm(wordIds(s)) += 1
        }
      }
      val text = hm.toSeq.sorted.map(e => (e._1 + ":" + e._2)).mkString(" ")
      if (hm.size > 3) {
        buf.append((usr, text))

      }
      else {
        ignored += 1
      }
    }
    println("Users ignored: " + ignored + ", available:" + buf.length)

    sc.makeRDD(buf)
  }


  def getFriendsOf(users: Set[String]): Set[String] = {
    val infoFile: List[(String, String)] = Source.fromFile(ClassifierData.infoFile).getLines().map(e => {
      val arr = e.split("\t")
      (arr(0), arr(1))
    }).toList
    val friends: Set[String] = infoFile.filter(e => users.contains(e._1)).map(e => e._2).toSet
    friends
  }
}
