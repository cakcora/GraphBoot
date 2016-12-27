import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
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
}
