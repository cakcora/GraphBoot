import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by cxa123230 on 2/2/2017.
  */
object Holder {
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
    val np: RDD[(String, Int, String)] = getNextTwets(sc, ClassifierData.getSeeds())
    val tempFile: String = "inputTemp.txt"
    val ww = new FileWriter(tempFile);
    for (n <- np.collect()) {
      ww.append(n._2 + " " + n._3 + "\r\n");
    }
    ww.close();
    val ndata = spark.read.format("libsvm").load(ClassifierData.outFile)
    val data = spark.read.format("libsvm").load(tempFile)


    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    val li = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(ndata)

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)
    val fi = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(ndata)

    // Split the data into training and test sets (30% held out for testing).
    //val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val trainingData = data
    val testData = ndata
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
    //val fullModel = pipeline.fit(data)

    // Make predictions.
    val predictions = model.transform(testData)
    predictions.select("predictedLabel", "label", "features").show(5)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))


  }

  def getNextTwets(sc: SparkContext, seeds: List[String]): RDD[(String, Int, String)] = {
    val f1: List[(String, String)] = Source.fromFile("userinfo.txt").getLines().map(e => {
      val arr = e.split("\t")
      (arr(0), arr(1))
    }).toList
    val nextUsers = f1.filter(e => seeds.contains(e._1)).map(e => e._2).filter(e => !seeds.contains(e))
    println(nextUsers.length + " users will be included.")
    val f2 = Source.fromFile("timeline.txt").getLines().map(e => {
      val arr = e.split("\t")
      (arr(0), arr(12))
    }).toList

    val nextTweets = f2.filter(e => nextUsers.contains(e._1)).map(e => (e._1, e._2))
    val stops = Source.fromFile("terrier-stop.txt").getLines().map(r => r.toString).toSet
    val tweets: RDD[(String, String)] = sc.makeRDD(nextTweets).map(e => (e._1, ClassifierData.clean(e._2, stops, ClassifierData.getMinWordLength()))).reduceByKey(_ + " " + _)
    println(tweets.count() + " users' tweets have been found.")
    val wordIds = Source.fromFile(ClassifierData.fwFile).getLines().map(e => e.split(":")).map(e => (e(0), e(1).toInt)).toMap
    val buf = scala.collection.mutable.ListBuffer.empty[(String, Int, String)]
    for (row <- tweets.collect()) {
      val usr = row._1
      val split: Array[String] = row._2.split(" ")
      val k1: RDD[(String, Int)] = sc.makeRDD(split.filter(e => wordIds.contains(e)).map(e => (e, 1))).reduceByKey(_ + _)

      val text = k1.map(e => (wordIds(e._1), e._2)).sortByKey(true).map(e => (e._1 + ":" + e._2)).collect().mkString(" ")
      if (split.length > 3)
        buf.append((usr, 0, text))
    }
    println(buf.length + " vectors will be classified")
    sc.makeRDD(buf)
  }


}
