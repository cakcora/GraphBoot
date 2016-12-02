import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/15/2016.
  */
object GraphCleaning {

  /*
  1- Remove multiple edges between vertices
  2- Remove self edges
   */
  def cleanGraph(sc:SparkContext, graph:Graph[Int,Int]): Graph[Int, Int] ={
    val j:Array[Edge[Int]] = graph.removeSelfEdges().edges.distinct().collect()
    val g: RDD[(VertexId, VertexId)] = graph.removeSelfEdges().edges.map(e => if (e.srcId > e.dstId) (e.dstId, e.srcId) else (e.srcId, e.dstId)).distinct()
    return Graph.fromEdgeTuples(g, defaultValue = 1)
  }

  def removeMultipleEdges2(sc:SparkContext, graph:Graph[Long,Int]): Graph[Long, Int] ={
    val j:Array[Edge[Int]] = graph.removeSelfEdges().edges.distinct().collect()

    val un:ListBuffer[(VertexId,VertexId)] = new ListBuffer[(VertexId, VertexId)]()
    for(a<-j){
      un.append(order(a))
    }
    val gr2: Graph[Long, Int]  =  Graph.fromEdgeTuples(sc.parallelize(un.distinct),defaultValue = 1)

    return gr2
  }

  def order(a: Edge[Int]): (VertexId, VertexId) = {
    if (a.srcId > a.dstId) {
      ((a.dstId, a.srcId))
    }
    else ((a.srcId, a.dstId))
  }


}
