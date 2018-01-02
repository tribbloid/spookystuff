package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.apache.spark.util.Utils
import org.json4s.Formats
import spire.ClassTag

import scala.language.implicitConversions
import scala.util.Try

abstract class MessageRelay[Proto: ClassTag] extends Codec[Proto] {

  override def selfType: ScalaType[Proto] = implicitly[ClassTag[Proto]]

  override def toMessage_>>(v: Proto): M = v match {
    case vv: ProtoAPI =>
      vv.toMessage_>>.asInstanceOf[M]
    case _ =>
      throw new UnsupportedOperationException(
        s"toMessage_>> is not implemented in ${this.getClass.getName} for message type ${this.messageMF.runtimeClass.getName}"
      )
  }

  override def toProto_<<(v: M): Proto = v match {
    case vv: MessageAPI_<< =>
      vv.toProto_<<.asInstanceOf[Proto]
    case _ =>
      throw new UnsupportedOperationException(
        s"toSelf_<< is not implemented in ${this.getClass.getName} for message type ${this.messageMF.runtimeClass.getName}"
      )
  }

  override def messageMF: Manifest[M] = intrinsicManifestTry.get

  //TODO: it only works if impl of MessageRelay is an object
  // maybe switching to M.<get companion class>?
  final lazy val intrinsicManifestTry: Try[Manifest[this.M]] = Try{

    val clazz = this.getClass
    val name = clazz.getName
    val modifiedName = name + "M"
    val reprClazz = Utils.classForName(modifiedName)

    Manifest.classType[this.M](reprClazz)
  }

  object CompanionReader extends MessageReader[M]()(messageMF) {
    override def formats: Formats = MessageRelay.this.formats

    //    override def fromJField(jf: JField): MessageRelay.this.M =
    //      MessageRelay.this.fromJField(jf)
  }
}
