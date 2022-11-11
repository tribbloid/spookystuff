package org.apache.spark.ml.dsl.utils

import java.security.Key

import javax.xml.bind.DatatypeConverter
import com.tribbloids.spookystuff.utils.EqualBy
import javax.crypto.spec.SecretKeySpec

import scala.language.implicitConversions

/**
  * Created by peng on 31/10/16.
  */
abstract class EncodedBinaryMagnet[T <: EncodedBinaryMagnet[T]] extends Product with Serializable with EqualBy {

  def asBytes: Array[Byte]

  def asBytesOrEmpty = Option(asBytes).getOrElse(Array.empty)

  @transient lazy val _equalBy: List[Byte] = asBytesOrEmpty.toList

  def strEncoding: String

  def strEncoding_nullSafe: String =
    if (asBytes == null) null
    else strEncoding

  final override def toString: String = "" + strEncoding_nullSafe

  def toSecretKeySpec(algorithm: String, lengthOpt: Option[Int] = None): SecretKeySpec = {
    val _blob = lengthOpt match {
      case Some(v) => asBytesOrEmpty.slice(0, v)
      case None    => asBytesOrEmpty
    }
    new SecretKeySpec(_blob, 0, asBytesOrEmpty.length, algorithm)
  }

  def toAESKey(lengthOpt: Option[Int] = Some(16)): SecretKeySpec = toSecretKeySpec("AES", lengthOpt)

  def map(f: String => String)(
      implicit
      ev: EncodedBinaryMagnet.Companion[T]
  ): T =
    ev.fromStr(f(this.strEncoding_nullSafe))

  def as[TT <: EncodedBinaryMagnet[TT]](
      implicit
      ev: EncodedBinaryMagnet.Companion[TT]
  ): TT = {
    ev.apply(asBytes)
  }
}

object EncodedBinaryMagnet {

  implicit def toBytes(v: EncodedBinaryMagnet[_]): Array[Byte] = v.asBytes

  implicit def toString(v: EncodedBinaryMagnet[_]): String = v.toString

  trait Companion[T <: EncodedBinaryMagnet[_]] {

    def apply(blob: Array[Byte]): T

    def _str2Bytes(str: String): Array[Byte]

    final def apply(str: String): T = {

      val blob = Option(str)
        .map(_str2Bytes)
        .getOrElse(
          null.asInstanceOf[Array[Byte]]
        )

      apply(blob)
    }

    implicit final def fromBytes(v: Array[Byte]): T = apply(v)

    implicit final def fromStr(v: String): T = apply(v)

    implicit final def fromKey(key: Key): T = {
      fromBytes(key.getEncoded)
    }

//    def fromPeers(v: EncodedBinaryMagnet[_]): T = fromStr(v.toString)

    implicit def ev: Companion[T] = this

    final lazy val Empty: T = apply(Array.empty[Byte])
    final lazy val Null: T = apply(null: Array[Byte])
  }

  object Base64 extends EncodedBinaryMagnet.Companion[Base64] {

    override def _str2Bytes(v: String) =
      DatatypeConverter.parseBase64Binary(v)
  }

  // TODO: this is defective as new Base64Magnet("AM28t0").asBase64Str = "AM28", same problem may happen to other impl, need to fix properly!
  case class Base64 private (asBytes: Array[Byte]) extends EncodedBinaryMagnet[Base64] {

    @transient override lazy val strEncoding: String = DatatypeConverter.printBase64Binary(asBytes)
    def asBase64Str: String = strEncoding
  }

  object Base16 extends EncodedBinaryMagnet.Companion[Base16] {

    override def _str2Bytes(v: String) =
      DatatypeConverter.parseHexBinary(v)
  }

  case class Base16 private (asBytes: Array[Byte]) extends EncodedBinaryMagnet[Base16] {

    @transient override lazy val strEncoding: String = DatatypeConverter.printHexBinary(asBytes)
    def asBase16Str: String = strEncoding
  }

  object UTF8 extends EncodedBinaryMagnet.Companion[UTF8] {

    override def _str2Bytes(v: String) =
      v.getBytes("UTF-8")
  }

  case class UTF8 private (asBytes: Array[Byte]) extends EncodedBinaryMagnet[UTF8] {

    @transient override lazy val strEncoding: String = new String(asBytes, "UTF-8")
    def asUTF8Str: String = strEncoding
  }
}
