import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, graphx}

import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/29/2016.
  */
object Common {
  def findWaveEdges(graph: Graph[Int, Int], seed: Int, wave: Int): RDD[Edge[Int]] = {
    //    if(wave<1) new SparkException("You requested an in valid subgraph, wave="+wave)
    var neigh0: Array[VertexId] = Array(seed)
    var d: RDD[Edge[Int]] = graph.edges.filter(e => (seed == e.dstId) || (seed == e.srcId))
    for (v <- 2 to wave) {
      neigh0 = d.flatMap(e => List(e.srcId, e.dstId)).distinct().collect()
      d = graph.edges.filter(e => neigh0.contains(e.dstId) || neigh0.contains(e.srcId))
    }
    return d
  }
  def subgraphWithWave(initialGraph: Graph[Int, Int], wave: Int): Graph[Int, Int] = {
    val dist = initialGraph.pregel(150)(
      (id, dist, newDist) => Math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        }
        else if (triplet.dstAttr + triplet.attr < triplet.srcAttr) {
          Iterator((triplet.srcId, triplet.dstAttr + triplet.attr))
        }
        else {
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
    val sampled: Array[(graphx.VertexId, Int)] = graph.vertices.takeSample(false, seedCount).map(e => (e._1, 0))
    sc.makeRDD(sampled)
  }

  def weightVertices(graph: Graph[PartitionID, PartitionID]): Graph[PartitionID, PartitionID] = {
    graph.mapVertices((id, _) => 1500)
  }
  def results(patchCount: PartitionID, graph: Graph[PartitionID, PartitionID], seedCount: PartitionID, patchDegrees: ListBuffer[Double], intervalLengths: ListBuffer[Double], degrees: Map[PartitionID, PartitionID]): String = {
    val denom: Double = math.pow(patchCount, 0.5)
    val nom: Double = Math.pow(intervalLengths.map(e => e * e).sum / patchCount, 0.5)
    val mean: Double = breeze.stats.mean(patchDegrees)

    val collect: List[Double] = degrees.map(e => e._2.toDouble).toList
    val avgGraphDeg: Double = breeze.stats.mean(collect)
    val cexp = (mean * denom - avgGraphDeg * denom) / nom
    val txt = (graph.numVertices + "\t" + graph.numEdges + "\t") + mean + "\t" + avgGraphDeg + "\t" + breeze.stats.mean(intervalLengths) + "\t" + breeze.stats.variance(patchDegrees) + "\t" + cexp
    return txt
  }
}
