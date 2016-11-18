import org.apache.spark.graphx.{Graph, PartitionStrategy}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by cxa123230 on 11/3/2016.
  */
object DryRunTestData {

  def getTestGraph(sc: SparkContext): Graph[Int, Int] = {
    val re: RDD[(Long, Long)] = sc.parallelize(Array((1L, 2L), (1L, 3L),
      (2L, 4L), (2L, 5L), (5L, 6L)))
    val tupleGraph = Graph.fromEdgeTuples(re, defaultValue = 1,
      uniqueEdges = Some(PartitionStrategy.RandomVertexCut))
    tupleGraph
  }
}
