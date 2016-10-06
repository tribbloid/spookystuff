package org.apache.spark.ml.shim;

case class MessageWriter(message: Message) extends MLWriter with Serializable {

  //    def saveJSON(path: String): Unit = {
  //      val resolver = HDFSResolver(sc.hadoopConfiguration)
  //
  //      resolver.output(path, overwrite = true){
  //        os =>
  //          os.write(StructRepr.this.prettyJSON.getBytes("UTF-8"))
  //      }
  //    }

  override protected def saveImpl(path: String): Unit = {

    val instance = new MessageParams(Identifiable.randomUID(Message.this.getClass.getSimpleName))

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata = Some("metadata" -> Message.this.toJValue))

    // Save stages
    //    val stagesDir = new Path(path, "stages").toString
    //    stages.zipWithIndex.foreach { case (stage: MLWritable, idx: Int) =>
    //      stage.write.save(getStagePath(stage.uid, idx, stages.length, stagesDir))
    //    }
  }
}
