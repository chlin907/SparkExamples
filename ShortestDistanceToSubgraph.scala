object ShortestDistanceToSubgraph {
  def apply(maxDist: Long = 1000, checpointFolderPath: String, maxIter: Int): DataFrame = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(checkpointFolderPath)
    if (fs.exists(outputPath))
      fs.delete(outputPath, true)

    sc.setCheckpointDir(checkpointFolderPath)

    val g: GraphFrame = examples.Graphs.friends  // get example graph
    val dfVert: DataFrame = g.vertices.withColumn("label", when('age >= 36, 0).otherwise(maxDist))
    val graph: GraphFrame = GraphFrame(dfVert, g.edges)

    val dfShortedDistance = graph.pregel.
                                  setMaxIter(maxIter).
                                  withVertexColumn("distance",
                                                   'label,
                                                   when((Pregel.msg + lit(1)) > 'distance, 'distance).
                                                   when(Pregel.msg.isNull, 'distance).
                                                   otherwise(Pregel.msg + lit(1))
                                                  ).
                                  sendMsgToDst(Pregel.src("distance")).
                                  aggMsgs(min(Pregel.msg)).
                                  run()

    if (fs.exists(outputPath))
      fs.delete(outputPath, true)

    dfShortedDistance
  }
}

// Example to run this function
// ShortestDistanceToSubgraph(maxDist = 1000, checkpointFolderPath, maxIter = 10)
