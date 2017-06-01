import scala.io.Source

val dir: String = "C://Projects/DisagioData/"
val f = Source.fromFile(dir + "epinionsarticletopic.txt").getLines().toList

val v = f.map(e => e.split("\t")).filter(f => f(2).toLong == 34252).map(e => (e(1), 1)).groupBy(_._1.toString).keySet



println(v.size)