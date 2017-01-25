import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD

/**
  * Created by cxa123230 on 11/15/2016.
  */
object GraphCleaning {

  /*
  1- Remove multiple edges between vertices
  2- Remove self edges
   */
  def cleanGraph(sc:SparkContext, graph:Graph[Int,Int]): Graph[Int, Int] ={
    val g: RDD[(VertexId, VertexId)] = graph.removeSelfEdges().edges.map(e => if (e.srcId > e.dstId) (e.dstId, e.srcId) else (e.srcId, e.dstId)).distinct()
    return Graph.fromEdgeTuples(g, defaultValue = 1)
  }

  def undirectedGraph(graph: Graph[Int, Int], i: Int): Graph[Int, Int] = {
    val g: RDD[(VertexId, VertexId)] = graph.edges.distinct().map(e => if (e.srcId > e.dstId) (e.dstId, e.srcId) else (e.srcId, e.dstId))
    val g2 = Graph.fromEdgeTuples(g, defaultValue = i, uniqueEdges = Some(PartitionStrategy.RandomVertexCut)).groupEdges((e1, e2) => (2))
    println(g2.numEdges + " directed edges")
    val g3 = Graph.fromEdges(g2.subgraph(epred = e => e.attr > 1).edges, defaultValue = i)
    return g3
  }

}
