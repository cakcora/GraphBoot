import org.apache.spark.graphx.{Graph, _}

import scala.collection.mutable.ListBuffer

/**
  * Created by cxa123230 on 11/29/2016.
  */
object Common {

  def results(patchCount: PartitionID, graph: Graph[PartitionID, PartitionID], seedCount: PartitionID, patchDegrees: ListBuffer[Double], intervalLengths: ListBuffer[Double], degrees: Map[PartitionID, PartitionID]): String = {
    val denom: Double = math.pow(patchCount, 0.5)
    val nom: Double = Math.pow(intervalLengths.map(e => e * e).sum / patchCount, 0.5)
    val mean: Double = breeze.stats.mean(patchDegrees)

    val collect: List[Double] = degrees.map(e => e._2.toDouble).toList
    val avgGraphDeg: Double = breeze.stats.mean(collect)
    val cexp = (mean * denom - avgGraphDeg * denom) / nom
    val txt = (graph.numVertices + "\t" + graph.numEdges + "\t")+ mean + "\t" + avgGraphDeg + "\t" + breeze.stats.variance(patchDegrees)+ "\t"+ cexp
    return txt
  }
}
