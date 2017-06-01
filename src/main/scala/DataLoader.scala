import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.{GraphGenerators, UpdatedGraphGenerators}
import org.apache.spark.graphx.{Graph, GraphLoader, PartitionStrategy}
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * Created by cxa123230 on 11/3/2016.
  */
object DataLoader {
  val dir: String = "C://Projects/DisagioData/"
  def getTestGraph(sc: SparkContext): Graph[Int, Int] = {
    val re: RDD[(Long, Long)] = sc.parallelize(Array((1L, 2L), (1L, 2L), (2L, 1L), (1L, 3L), (1L, 3L), (4L, 2L), (2L, 4L), (2L, 5L), (5L, 4L), (5L, 6L), (4L, 7L)))
    val tupleGraph = Graph.fromEdgeTuples(re, defaultValue = 1,
      uniqueEdges = Some(PartitionStrategy.RandomVertexCut))
    tupleGraph
  }

  def loadData(sc: SparkContext, value: Int) = {
    val f = Source.fromFile(dir + "epinionsarticletopiccleaned.txt").getLines().toList

    f.map(e => e.split("\t")).filter(e => e.length == 3).filter(f => f(2).toLong == value).map(e => (e(1), 1)).groupBy(_._1.toString).map(x => (x._1.toInt, 1 + x._2.size)).withDefaultValue(1)
  }

  def loadGraph(sc: SparkContext, graphType: String, options: Map[String, AnyVal]): Graph[Int, Int] = {

    graphType match {
      case "grid" => {
        val dim = Math.pow(options("vertices").asInstanceOf[Int], 0.5).toInt
        val g: Graph[(Int, Int), Double] = GraphGenerators.gridGraph(sc, dim, dim)
        val gra: Graph[Int, Int] = g.mapVertices((a, b) => 1).mapEdges(a => 1)
        GraphCleaning.cleanGraph(sc, gra)
      }
      case "lognormal" => {
        val mu = options("mu").asInstanceOf[Double]
        val sigma = options("sigma").asInstanceOf[Double]
        val vertices = options("vertices").asInstanceOf[Int]
        val gr: Graph[Long, Int] = UpdatedGraphGenerators.logNormalGraph(sc, vertices, 1, mu, sigma).removeSelfEdges()
        GraphCleaning.cleanGraph(sc, gr.mapVertices((a, b) => a.toInt))

      }
      case "epinions-value" => {
        GraphLoader.edgeListFile(sc, dir + "epinionsuser_ratingcleaned.txt")
      }
      case "rmat" =>{
        val requestedNumVertices: Int = options("vertices").asInstanceOf[Int]
        val edgeDensity: Int = options("edgeDensity").asInstanceOf[Int]
        GraphGenerators.rmatGraph(sc, requestedNumVertices, edgeDensity * requestedNumVertices)
      }
      case "dblp" => {
        GraphLoader.edgeListFile(sc, dir + "dblpgraph.txt")
      }
      case "facebook" => {
        GraphLoader.edgeListFile(sc, dir + "facebook-links.txt")
      }
      case "facebook-ego" => {
        GraphLoader.edgeListFile(sc, dir + "facebook_combined.txt")
      }
      case "enron" => {
        GraphLoader.edgeListFile(sc, dir + "Email-Enron.txt")
      }
      case "wiki-talk" => {
        GraphLoader.edgeListFile(sc, dir + "WikiTalk.txt")
      }
      case "wiki-vote" => {
        GraphLoader.edgeListFile(sc, dir + "Wiki-Vote.txt")
      }
      case "gowalla" => {
        GraphLoader.edgeListFile(sc, dir + "Gowalla_edges.txt")
      }
      case "kite" => {
        GraphLoader.edgeListFile(sc, dir + "Brightkite_edges.txt")
      }
      case "epinions" => {
        GraphLoader.edgeListFile(sc, dir + "soc-Epinions1.txt")
      }
      case "slashdot+" => {
        GraphLoader.edgeListFile(sc, dir + "slashdot-zoo-pos.txt")
      }
      case "slashdot-" => {
        GraphLoader.edgeListFile(sc, dir + "slashdot-zoo-neg.txt")
      }
      case "prosper" => {
        GraphLoader.edgeListFile(sc, dir + "prosper-loans.txt")
      }
      case "livemocha" => {
        GraphLoader.edgeListFile(sc, dir + "livemocha.txt")
      }
      case "douban" => {
        GraphLoader.edgeListFile(sc, dir + "out.douban")
      }
      case "libimseti" => {
        GraphLoader.edgeListFile(sc, dir + "out.libimseti")
      }
      case "digg-friends" => {
        GraphLoader.edgeListFile(sc, dir + "out.digg-friends")
      }
      case "actor-co" => {
        GraphLoader.edgeListFile(sc, dir + "out.actor-collaboration")
      }
      case "citeser" => {
        GraphLoader.edgeListFile(sc, dir + "out.citeseer")
      }
      case "dogster" => {
        GraphLoader.edgeListFile(sc, dir + "out.dogster")
      }
      case "catster" => {
        GraphLoader.edgeListFile(sc, dir + "out.catster")
      }
      case "hamster" => {
        GraphLoader.edgeListFile(sc, dir + "out.hamster")
      }
      case "chicago" => {
        GraphLoader.edgeListFile(sc, dir + "out.chicago")
      }
      case "california" => {
        GraphLoader.edgeListFile(sc, dir + "out.california")
      }
      case "texas" => {
        GraphLoader.edgeListFile(sc, dir + "out.texas")
      }
      case "jdk" => {
        GraphLoader.edgeListFile(sc, dir + "out.jdk")
      }
      case "jung" => {
        GraphLoader.edgeListFile(sc, dir + "out.jung")
      }
      case "linux" => {
        GraphLoader.edgeListFile(sc, dir + "out.linux")
      }
      //-------------------------------------------
      case "patent" => {
        GraphLoader.edgeListFile(sc, dir + "cit-Patents.txt")
      }
      case "livejournal" => {
        GraphLoader.edgeListFile(sc, dir + "com-lj.ungraph.txt")
      }
      case "as-skitter" => {
        GraphLoader.edgeListFile(sc, dir + "as-skitter.txt")
      }
      case "orkut" => {
        GraphLoader.edgeListFile(sc, dir + "com-orkut.ungraph.txt")
      }
      case "youtube" => {
        GraphLoader.edgeListFile(sc, dir + "com-youtube.ungraph.txt")
      }
      case "amazon" => {
        GraphLoader.edgeListFile(sc, dir + "com-amazon.ungraph.txt")
      }
      //------------------------------
      case "web-google" => {
        GraphLoader.edgeListFile(sc, dir + "web-Google.txt")
      }
      case "web-notredame" => {
        GraphLoader.edgeListFile(sc, dir + "web-NotreDame.txt")
      }
      case "web-berkeley" => {
        GraphLoader.edgeListFile(sc, dir + "web-BerkStan.txt")
      }
      case "web-stanford" => {
        GraphLoader.edgeListFile(sc, dir + "web-Stanford.txt")
      }
      //-----------------------------------------------------------
      case "Ca-CondMat" => {
        GraphLoader.edgeListFile(sc, dir + "CA-CondMat.txt")
      }
      case "CA-AstroPh" => {
        GraphLoader.edgeListFile(sc, dir + "CA-AstroPh.txt")
      }
      case "CA-GrQc" => {
        GraphLoader.edgeListFile(sc, dir + "CA-GrQc.txt")
      }
      case "CA-HepPh" => {
        GraphLoader.edgeListFile(sc, dir + "CA-HepPh.txt")
      }
      case "CA-HepTh" => {
        GraphLoader.edgeListFile(sc, dir + "CA-HepTh.txt")
      }
      case _: String => {
        println("No preference for graph type: Using a random star graph.")
        GraphGenerators.starGraph(sc, 100)
      }
    }
  }
}
