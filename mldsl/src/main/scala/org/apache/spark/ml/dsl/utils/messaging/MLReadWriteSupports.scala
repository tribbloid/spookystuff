package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.json4s.JsonAST.JObject

/**
  * Created by peng on 05/10/16.
  */
object MLReadWriteSupports {

  class MsgReadable[Obj: Codec]() extends MLReadable[Obj] {

    //    object FlowReader extends MLReader[Flow] {
    //
    //      /** Checked against metadata when loading model */
    //      private val className = classOf[Flow].getName
    //
    //      override def load(path: String): Flow = {
    //        val (
    //          uid,
    //          stages
    //          ) = SharedReadWrite.load(className, this.sc, path) //TODO: not sure if it can be reused
    //
    //        null
    //      }
    //    }

    override def read = {

      implicitly[Codec[Obj]].toMLReader
    }
  }

  implicit class MsgWritable[Obj: Codec](v: Obj) extends MLWritable {

    override def write = {

      implicitly[Codec[Obj]].toMLWriter(v)
    }
  }

  implicit class ReadWrite[Obj](val base: Codec[Obj]) {

    def toMLReader: MLReader[Obj] = MessageMLReader(base)

    def toMLWriter(v: Obj) = {
      MessageMLWriter(base.toWriter_>>(v))
    }
  }
}

case class MessageMLReader[Obj](outer: Codec[Obj]) extends MLReader[Obj] {

  //TODO: need impl
  override def load(path: String): Obj = {
    //      val metadata = DefaultParamsReader.loadMetadata(path, sc)
    //      val cls = Utils.classForName(metadata.className)
    //      val instance =
    //        cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
    //      DefaultParamsReader.getAndSetParams(instance, metadata)
    //      instance.asInstanceOf[T]
    ???
  }
}

case class MessageMLWriter[T](message: MessageWriter[T]) extends MLWriter with Serializable {

  //    def saveJSON(path: String): Unit = {
  //      val resolver = HDFSResolver(sc.hadoopConfiguration)
  //
  //      resolver.output(path, overwrite = true){
  //        os =>
  //          os.write(StructRepr.this.prettyJSON.getBytes("UTF-8"))
  //      }
  //    }

  override protected def saveImpl(path: String): Unit = {

    val instance = new MessageParams(Identifiable.randomUID(message.message.getClass.getSimpleName))

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
