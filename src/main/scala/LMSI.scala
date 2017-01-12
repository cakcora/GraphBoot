import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by cxa123230 on 11/25/2016.
  */
object LMSI {
  def parallelLMSI(graph: Graph[Int, Int], seeds: RDD[(Long, Int)], wave: Int): List[Int] = {

    val weightedGraph: Graph[PartitionID, PartitionID] = Common.weightVertices(graph)
    val initialGraph = weightedGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
    val k: Graph[PartitionID, PartitionID] = Common.subgraphWithWave(initialGraph, wave)
    val seedList = seeds.map(e => e._1.toInt).collect().toList
    val edges: ListBuffer[(PartitionID, PartitionID)] = k.edges.map(e => (e.srcId.toInt, e.dstId.toInt)).collect().to[ListBuffer]

    val seenVertices: mutable.HashSet[PartitionID] = new mutable.HashSet[PartitionID]()
    val initialVertexList: ListBuffer[PartitionID] = new ListBuffer[PartitionID]()
    for (seed <- seedList) {
      initialVertexList.append(seed)
      seenVertices.add(seed)
    }
    val outputList = lmsiAlgorithm(wave, edges, seenVertices, initialVertexList)
    outputList
  }

  def lmsiAlgorithm(wave: PartitionID, remainingEdges: ListBuffer[(PartitionID, PartitionID)], seenVertices: mutable.HashSet[PartitionID], outputVertexList: ListBuffer[PartitionID]): List[Int] = {
    var w = 0;
    while (!remainingEdges.isEmpty && w < wave) {
      val phase: mutable.HashSet[PartitionID] = new mutable.HashSet[PartitionID]()
      w += 1
      var rList: mutable.Set[(PartitionID, PartitionID)] = new mutable.HashSet[(PartitionID, PartitionID)]()
      for ((a, b) <- remainingEdges) {
        var f = false
        if (seenVertices.contains(a)) {
          f = true
          if (seenVertices.contains(b)) {
            outputVertexList.append(b)
            outputVertexList.append(a)
          }
          else {
            outputVertexList.append(b)
            phase.add(b)
          }
        }
        else if (seenVertices.contains(b)) {
          f = true
          outputVertexList.append(a)
          phase.add(a)
        }
        if (f) {
          rList.add((a, b))
        }
      }
      seenVertices ++= phase
      remainingEdges --= rList
    }
    outputVertexList.toList
  }

  def sequentialLMSI(sc: SparkContext, graph: Graph[Int, Int], seedArray: Array[(VertexId, Int)], wave: Int): List[Int] = {
    val seeds = sc.makeRDD(seedArray)
    val seedList = seeds.map(e => e._1.toInt).collect().toList
    val weightedGraph: Graph[Int, Int] = Common.weightVertices(graph)
    val initialGraph = weightedGraph.joinVertices(seeds)((x, c, v) => Math.min(c, v))
    val fut: Future[List[List[Int]]] = Future.traverse(seedList) { i =>
      val localEdges: RDD[Edge[Int]] = Common.findWaveEdges(initialGraph, i, wave)
      Future {
        LMSI.singleSeed(localEdges, i, wave)
      }
    }
    Await.result(fut, Duration.Inf).flatten
  }

  def singleSeed(edgeRDD: RDD[Edge[Int]], seed: Int, wave: Int): List[Int] = {

    val edges: ListBuffer[(Int, Int)] = edgeRDD.map(e => (e.srcId.toInt, e.dstId.toInt)).collect().to[ListBuffer]

    val seenVertices: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    val initialVertexList: ListBuffer[Int] = new ListBuffer[Int]()
    initialVertexList.append(seed)
    seenVertices.add(seed)

    val outputList = lmsiAlgorithm(wave, edges, seenVertices, initialVertexList)
    outputList
  }
}
