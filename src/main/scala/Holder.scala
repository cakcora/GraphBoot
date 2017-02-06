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
    val wave1Profile: Array[(String, String)] = getWave1Profile(sc, ClassifierData.getSeeds()).collect()
    val inputFileForClasification: String = "inputTemp.txt"
    val wave0Profile = Source.fromFile(ClassifierData.outFile).getLines()
    val inputFileHandle = new FileWriter(inputFileForClasification);
    for (n <- wave0Profile) {
      inputFileHandle.append(n + "\r\n");
    }
    val idMap = mutable.HashMap.empty[Int, String]
    var givenId = 10
    for (userProfile <- wave1Profile) {
      inputFileHandle.append(givenId + " " + userProfile._2 + "\r\n");
      idMap(givenId) = userProfile._1
      givenId += 1
    }
    inputFileHandle.close();
    val data = spark.read.format("libsvm").load(inputFileForClasification)



    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.


    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)


    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)


    // Split the data into training and test sets (30% held out for testing).
    //val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val trainingData: Dataset[Row] = data.filter(e => e.get(0) == 1 || e.get(0) == 0)
    val testData: Dataset[Row] = data.filter(e => e.get(0) != 1 && e.get(0) != 0)
    println("Training on " + trainingData.count() + " testing on :" + testData.count())
    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)
    val ow = new FileWriter(ClassifierData.predResultFile)
    savePredictionsToFile(idMap, predictions, ow)


  }

  def savePredictionsToFile(idMap: mutable.HashMap[Int, String], predictions: DataFrame, ow: FileWriter): Unit = {
    val preds: Array[Row] = predictions.rdd.collect()

    predictions.printSchema()
    println(predictions.first().getAs[Double]("label"))
    for (row <- preds) {

      val lb = idMap(row.getAs[Double]("label").toInt)
      val pl = row.getAs[String]("predictedLabel")

      ow.append(lb + "\t" + pl + "\r\n")
    }
    ow.close
  }

  def getWave1Profile(sc: SparkContext, seeds: Set[String]): RDD[(String, String)] = {
    val infoFile: List[(String, String)] = Source.fromFile(ClassifierData.infoFile).getLines().map(e => {
      val arr = e.split("\t")
      (arr(0), arr(1))
    }).toList
    val nextUsers: List[String] = infoFile.filter(e => seeds.contains(e._1)).map(e => e._2).filter(e => !seeds.contains(e))

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


}
