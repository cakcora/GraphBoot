import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, graphx}

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

  def chooseKnownSeedsExp(sc: SparkContext, seeds: Map[Int, Int], seedCount: Int): RDD[(graphx.VertexId, Int)] = {
    val sampled: Array[(graphx.VertexId, Int)] = seeds.keySet.take(seedCount).map(e => (e.toLong, 0)).toArray
    sc.makeRDD(sampled)
  }

  def weightVertices(graph: Graph[PartitionID, PartitionID]): Graph[PartitionID, PartitionID] = {
    graph.mapVertices((id, _) => 1500)
  }

  def proxyMu(seeds: Array[Int], degreeMap: Map[Int, Int]): Double = {
    val degreeList = degreeMap.filter(e => seeds.contains(e._1)).map(e => e._2.toDouble)
    val d = breeze.stats.meanAndVariance(degreeList)
    return d.mean
  }


}
