package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.apache.spark.util.Utils
import org.json4s.Formats
import spire.ClassTag

import scala.language.implicitConversions
import scala.util.Try

abstract class MessageRelay[Self: ClassTag] extends Codec[Self] {

  override def selfType: ScalaType[Self] = implicitly[ClassTag[Self]]

  override def toSelf_<<(v: M): Self = v match {
    case vv: MessageAPI_<=>[Self] =>
      vv.toSelf_<<
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
