import java.io.FileWriter

import breeze.stats.DescriptiveStats
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, graphx}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by cxa123230 on 11/3/2016.
  */
object GraphBootApproach2 {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[16]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger().setLevel(Level.ERROR)
    val fw = new FileWriter("approach1.txt") ;
    val header = "mu\\tsigma\\tvertices\\tseedCount\\tbootCount\\tpatchCount\\tpx\\tnumVertices\\tnumEdges\\tmean\\tavgGraphDeg\\tvarianceOfpatchDegrees\\tC\n"
    fw.write(header) ;

    var wave: PartitionID = 3
    var bootCount: PartitionID = 10
    var patchCount:PartitionID = 10
    var mu =3
    var sigma = 1.3
    var vertices = 1000
    var sx=20
    var px=40

    for(px<-10 to 100 by 10) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    px=40
    for(sx<-20 to 100 by 10) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    sx=20
    for(mu<-1.0 to 5.0 by 0.5) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    mu=3
    for(sigma<-0.0 to 2.0 by 0.2) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    sigma=1.3
    for(vertices<-10000 to 100000 by 10000) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    vertices = 10000
    for(wave<-1 to 4 by 1) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    wave =3
    for(patchCount<-1 to 10 by 1) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    patchCount =10;
    for(bootCount<-1 to 1000 by 50) {
      val txt = GraphBoot(px,mu, sigma, vertices, sc, sx, patchCount, wave, bootCount)
      fw.write(mu + "\t" + sigma + "\t" + vertices + "\t" + sx + "\t" + bootCount + "\t" + patchCount + "\t" + px + "\t" + txt + "\n")
      fw.flush()
    }
    bootCount=10;
    fw.close()
    sc.stop()
  }


  def GraphBoot(px:Int, mu:Double, sigma:Double, vertices:Int, sc:SparkContext, sx:Int, patchCount:Int, wave:Int, bootCount:Int):String = {
    val options = Map(("mu", mu), ("sigma", sigma), ("vertices", vertices))
    val graph: Graph[PartitionID, PartitionID] = GraphCleaning.cleanGraph(sc, SyntheticData.synthGraphGenerator(sc, "lognormal", options))
    println("Graph created.")
    var seedCount: PartitionID = (graph.numVertices / sx).toInt

    var patchDegrees: ListBuffer[Double] = new ListBuffer[Double]()
    val intervalLengths: ListBuffer[Double] = new ListBuffer[Double]()
    val degrees: Map[Int, Int] = graph.collectNeighborIds(EdgeDirection.Either).collect().map(e => e._1.toInt -> e._2.length).toMap
    for (j <- 1 to patchCount) {
      println("patch "+j)
      val seeds: RDD[(VertexId, Int)] = chooseSeeds(sc, graph, seedCount)
      var initialGraph: Graph[Int, Int] = graph.mapVertices((id, _) => 1500)
      initialGraph = initialGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
      val subGraph: Graph[Int, Int] = subgraphWithWave(initialGraph, wave)
      val aList: Set[Edge[Int]] = subGraph.edges.collect().toSet
      var mList: mutable.Set[Pair[Int, Int]] = new mutable.HashSet[Pair[Int, Int]]()
      for (a <- aList) {
        mList.add((a.srcId.toInt, a.dstId.toInt))
      }

      val subList: ListBuffer[Int] = new ListBuffer[Int]()
      for (seed <- seeds.map(e => e._1.toInt).collect().toList) {
        subList ++= LMSI.singleSeed(mList.clone(), seed)
      }

      val proxySampleSize: PartitionID = 1 + (subGraph.numVertices / px).toInt
      val vertexList: List[PartitionID] = subList.toList

      val seedSet: Set[PartitionID] = seeds.map(e => e._1.toInt).collect().toSet
      val bstrapDegrees: List[Double] = BootStrapper.boot(bootCount, proxySampleSize, vertexList, degrees, seedSet)
      val dc = (i: Double) => {
        DescriptiveStats.percentile(bstrapDegrees, i)
      }
      val length: Double = 0.5 * (dc(0.95) - dc(0.05))
      val M: Double = dc(0.5)
      patchDegrees += M;
      intervalLengths += length
      println("Patch M:" + M + " W:" + length)
    }
    val txt = Common.results(patchCount, graph, seedCount, patchDegrees, intervalLengths, degrees)
    return txt

  }





  def subgraphWithWave(initialGraph: Graph[Int, Int], wave: Int): Graph[Int, Int] = {
    val dist = initialGraph.pregel(Int.MaxValue)(
      (id, dist, newDist) => Math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )
    val subGraph = dist.subgraph(vpred = ((vertexId, vertexDistance) => {
      vertexDistance <= wave
    }))
    subGraph
  }


  def chooseSeeds(sc: SparkContext, graph: Graph[Int, Int], seedCount: Int): RDD[(graphx.VertexId, Int)] = {
    val vList: List[Int] = graph.vertices.collect().map(x => x._1.toInt).toList
    val size = vList.length
    val random = new Random()
    var seedList: ListBuffer[(Long, Int)] = ListBuffer()
    for (i <- 1 to seedCount) {
      seedList += Tuple2(vList(random.nextInt(size)), 0)
    }
    sc.parallelize(seedList)
  }


}
