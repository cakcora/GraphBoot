import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Set


val gr = Set[(Int,Int)]()

gr.add(1,2)
gr.add(1,3)
gr.add(1,5)
gr.add(1,3)
println(gr.size)
