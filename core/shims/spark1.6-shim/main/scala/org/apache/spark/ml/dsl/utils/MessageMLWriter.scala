package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsWriter, Identifiable, MLWriter}
import org.json4s.JsonAST.JObject

case class MessageMLWriter(message: MessageAPI) extends MLWriter with Serializable {

  //    def saveJSON(path: String): Unit = {
  //      val resolver = HDFSResolver(sc.hadoopConfiguration)
  //
  //      resolver.output(path, overwrite = true){
  //        os =>
  //          os.write(StructRepr.this.prettyJSON.getBytes("UTF-8"))
  //      }
  //    }

  override protected def saveImpl(path: String): Unit = {

    val instance = new MessageParams(Identifiable.randomUID(message.getClass.getSimpleName))

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata = Some(JObject("metadata" -> message.toJValue)))

    // Save stages
    //    val stagesDir = new Path(path, "stages").toString
    //    stages.zipWithIndex.foreach { case (stage: MLWritable, idx: Int) =>
    //      stage.write.save(getStagePath(stage.uid, idx, stages.length, stagesDir))
    //    }
  }
}

class MessageParams(
                     val uid: String
                   ) extends Params {

  override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
}
