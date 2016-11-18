import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}

/**
  * Created by cxa123230 on 11/15/2016.
  */
object GraphCleaning {
  def removeMultipleEdges(sc:SparkContext, graph:Graph[Int,Int]): Graph[Int, Int] ={
    val setA:scala.collection.mutable.Set[(Long,Long)] =scala.collection.mutable.Set[(Long,Long)]()
    val j:Array[Edge[Int]] = graph.edges.distinct().collect()

    val gr2: Graph[Int, Int]  = Graph.fromEdges(sc.parallelize(j),defaultValue = 1)

    return gr2
  }


}
