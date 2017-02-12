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
  * requires data output from Classifier.Data
  */
object Holder {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  val featureFile: String = "results/classified/1featureWords.txt"
  val inputFile: String = "results/classified/1inputGB.txt"
  val prevPred: String = "results/classified/0resultPredsall.txt"
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
    println(seeds.size + " seeds")
    val wave1: Set[String] = getFriendsOf(seeds).diff(seeds)
    println(wave1.size + " friends of seeds.")
    val prevLabels = Source.fromFile(prevPred).getLines().toList
    val prevDepusers: Set[String] = prevLabels.map(e => e.split("\t")).filter(f => f(1) == "1.0").map(e => e(0)).toSet
    println(prevDepusers.size + " depressed users from previous labeling. This includes waves one and two")
    val wave1AndDepressed: Set[String] = wave1 //.intersect(prevDepusers)
    println(wave1AndDepressed.size + " wave one users were found to be depressed.")
    val wave2: Set[String] = getFriendsOf(wave1AndDepressed).diff(wave1).diff(seeds)
    println(wave2.size + " new wave two users found will be classified")
    val wave2Profiles: Array[(String, String)] = getProfiles(sc, wave2).collect()
    println(wave2Profiles.size + " profiles of these new wave two users were found.")

    val inputFileForClasification: String = "inputTemp.txt"
    val labeledData = Source.fromFile(inputFile).getLines().toList
    val tempFile = new FileWriter(inputFileForClasification);
    println(labeledData.size + " labeled data found.")
    for (n <- labeledData) {
      tempFile.append(n + "\r\n");
    }
    val idMap = mutable.HashMap.empty[Int, String]
    var assignedId = 10 //ids start from 10
    for (userProfile <- wave2Profiles) {
      tempFile.append(assignedId + " " + userProfile._2 + "\r\n");
      idMap(assignedId) = userProfile._1
      assignedId += 1
    }
    tempFile.close();
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

  def getProfiles(sc: SparkContext, nextUsers: Set[String], returnRawProfiles: Boolean = false): RDD[(String, String)] = {

    val tweetFile: List[(String, String)] = Source.fromFile(ClassifierData.tweetFile).getLines().map(e => {
      val arr = e.split("\t")
      (arr(0), arr(12))
    }).toList
    val nextTweets: List[(String, String)] = tweetFile.filter(e => nextUsers.contains(e._1)).map(e => (e._1, e._2))
    val stops: Set[String] = Source.fromFile(ClassifierData.stopFile).getLines().map(r => r.toString).toSet
    val tw: RDD[(String, String)] = sc.makeRDD(nextTweets)

    if (returnRawProfiles) return tw.reduceByKey(_ + " " + _)

    val tweets: Array[(String, String)] = tw.map(e => (e._1, ClassifierData.clean(e._2, stops, ClassifierData.getMinWordLength()))).reduceByKey(_ + " " + _).collect()


    val wordIds = Source.fromFile(featureFile).getLines().map(e => e.split(":")).map(e => (e(0), e(1).toInt)).toMap
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
