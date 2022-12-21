package org.apache.spark.ml.dsl.utils.messaging.io

import org.apache.spark.ml.dsl.utils.messaging.{IR, Relay}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.json4s.JObject

import scala.language.existentials

/**
  * Created by peng on 05/10/16.
  */
object MLReadWriteSupports {

  class MsgReadable[Obj: Relay]() extends MLReadable[Obj] {

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

      implicitly[Relay[Obj]].toMLReader
    }
  }

  implicit class MsgWritable[Obj: Relay](v: Obj) extends MLWritable {

    override def write = {

      implicitly[Relay[Obj]].toMLWriter(v)
    }
  }

  implicit class ReadWrite[Obj](val relay: Relay[Obj]) {

    def toMLReader: MLReader[Obj] = Reader(relay)

    def toMLWriter(v: Obj): Writer[relay.IR_>>] = {
      val enc: Encoder[relay.IR_>>] = relay.toEncoder_>>(v)
      Writer(enc)
    }
  }

  case class Reader[Obj](outer: Relay[Obj]) extends MLReader[Obj] {

    // TODO: need impl
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

  case class Writer[T <: IR](message: Encoder[T]) extends MLWriter with Serializable {

    //    def saveJSON(path: String): Unit = {
    //      val resolver = HDFSResolver(sc.hadoopConfiguration)
    //
    //      resolver.output(path, overwrite = true){
    //        os =>
    //          os.write(StructRepr.this.prettyJSON.getBytes("UTF-8"))
    //      }
    //    }

    override protected def saveImpl(path: String): Unit = {

      val instance = new _Params(Identifiable.randomUID(message.ir.body.getClass.getSimpleName))

      val jV = JObject("metadata" -> message.toJValue)
      DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata = Some(jV))

      // Save stages
      //    val stagesDir = new Path(path, "stages").toString
      //    stages.zipWithIndex.foreach { case (stage: MLWritable, idx: Int) =>
      //      stage.write.save(getStagePath(stage.uid, idx, stages.length, stagesDir))
      //    }
    }
  }

  class _Params(
      val uid: String
  ) extends Params {

    override def copy(extra: ParamMap): Params = this.defaultCopy(extra)
  }

}
