package com.tribbloids.spookystuff.relay.io

import com.tribbloids.spookystuff.relay.{IR, Relay}
import org.apache.spark.ml._MLHelper
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.json4s.JObject

/**
  * Created by peng on 05/10/16.
  */
object MLReadWriteSupports {

  class MsgReadable[Obj: Relay]() extends MLReadable[Obj] {

    override def read: MLReader[Obj] = {

      implicitly[Relay[Obj]].toMLReader
    }
  }

  implicit class MsgWritable[Obj: Relay](v: Obj) extends MLWritable {

    override def write: com.tribbloids.spookystuff.relay.io.MLReadWriteSupports.Writer[_1.relay.IR_>>] forSome {
      val _1: com.tribbloids.spookystuff.relay.io.MLReadWriteSupports.ReadWrite[Obj]
    } = {

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

    override protected def saveImpl(path: String): Unit = {

      val instance = new _Params(Identifiable.randomUID(message.ir.body.getClass.getSimpleName))

      val jV = JObject("metadata" -> message.toJValue)
      _MLHelper._DefaultParamsWriter.saveMetadata(instance, path, sc, Some(jV), None)

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
