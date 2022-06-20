import org.apache.spark.graphx.GraphLoader
val graph = GraphLoader.edgeListFile(sc, "p2p-Gnutella08.txt")
val t1 = System.nanoTime
val ranks = graph.pageRank(0.0001).vertices
val duration = (System.nanoTime - t1) / 1e9d
println("Total time ", duration)


val users = sc.textFile("users1.txt").map { line =>
  val fields = line.split("\t")
  (fields(0).toLong, fields(1))
}
val ranksByUsername = users.join(ranks).map {
  case (id, (username, rank)) => (username, rank)
}

ranksByUsername.coalesce(1).saveAsTextFile("scala_output")
println(ranksByUsername.collect().mkString("\n"))



