import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages
import org.graphframes.lib.Pregel
import org.graphframes.examples
import org.apache.hadoop.fs.{FileSystem, Path}

object PageRankExample {

  def apply(alpha: Double = 0.15, checpointFolderPath: String): DataFrame = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(checkpointFolderPath)
    if (fs.exists(outputPath))
      fs.delete(outputPath, true)

    val g: GraphFrame = examples.Graphs.friends  // get example graph
    val dfVert = g.outDegrees
    val dfEdge = g.edges
    val numVertices: Long = dfVert.count()

    val graph = GraphFrame(dfVert, dfEdge)
    val dfPageRank = graph.pregel.
                           withVertexColumn("rank",
                                            lit(1.0 / numVertices),
                                            coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices
                                           ).
                           sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree")).
                           aggMsgs(sum(Pregel.msg)).
                           run()

    if (fs.exists(outputPath))
      fs.delete(outputPath, true)

    dfPageRank
  }
}
