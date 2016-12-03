import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, GraphLoader, PartitionStrategy}
import org.apache.spark.rdd.RDD

/**
  * Created by cxa123230 on 11/3/2016.
  */
object SyntheticData {

  def getTestGraph(sc: SparkContext): Graph[Int, Int] = {
    val re: RDD[(Long, Long)] = sc.parallelize(Array((1L, 2L), (1L, 3L),
      (2L, 4L), (2L, 5L), (5L, 6L)))
    val tupleGraph = Graph.fromEdgeTuples(re, defaultValue = 1,
      uniqueEdges = Some(PartitionStrategy.RandomVertexCut))
    tupleGraph
  }
  def synthGraphGenerator(sc: SparkContext, graphType: String, options:Map[String, AnyVal]): Graph[Int, Int] = {

    graphType match {
      case "grid" => {
        val dim = Math.pow(options("vertices").asInstanceOf[Int], 0.5).toInt
        val g: Graph[(Int, Int), Double] = GraphGenerators.gridGraph(sc, dim, dim)
        val gra: Graph[Int, Int] = g.mapVertices((a, b) => 1).mapEdges(a => 1)
        GraphCleaning.cleanGraph(sc, gra)
      }
      case "lognormal" => {
        val mu = options("mu").asInstanceOf[Double]
        val sigma = options("sigma").asInstanceOf[Double]
        val vertices = options("vertices").asInstanceOf[Int]
        val gr: Graph[Long, Int] = GraphGenerators.logNormalGraph(sc, vertices, 1, mu, sigma).removeSelfEdges()
      GraphCleaning.cleanGraph(sc, gr.mapVertices((a, b) => a.toInt))

      }
      case "rmat" =>{
        GraphGenerators.rmatGraph(sc,1000, 15000)
      }
      case "dblp" => {
        GraphLoader.edgeListFile(sc, "src/main/resources/dblpgraph.txt")
      }
      case _: String => {
        println("No preference for graph type: Using a random star graph.")
        GraphGenerators.starGraph(sc, 100)
      }
    }
  }
}
