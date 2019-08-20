package org.apache.spark.ml.dsl.utils

import java.security.Key

import javax.xml.bind.DatatypeConverter
import com.tribbloids.spookystuff.utils.IDMixin
import javax.crypto.spec.SecretKeySpec

import scala.language.implicitConversions

/**
  * Created by peng on 31/10/16.
  */
//TODO: should use magnet pattern
class ByteEncoded(
    blob: Array[Byte]
) extends IDMixin {

  def asBytes: Array[Byte] = blob

  lazy val _id = blob.toList

  lazy val toBase64Str = DatatypeConverter.printBase64Binary(blob)
  lazy val toBase16Str = DatatypeConverter.printHexBinary(blob)
  lazy val toUTF8Str = new String(blob, "UTF-8")

  override def toString: String = toBase64Str

  def toKeySpec(algorithm: String, lengthOpt: Option[Int] = None) = {
    val _blob = lengthOpt match {
      case Some(v) => blob.slice(0, v)
      case None    => blob
    }
    new SecretKeySpec(_blob, 0, blob.length, algorithm)
  }

  def toAESKey(lengthOpt: Option[Int] = Some(16)) = toKeySpec("AES", lengthOpt)

  lazy val copyConstructor = this.getClass.getConstructor(classOf[Array[_]])
  // TODO: this may be slower than compiled function
  def copy(blob: Array[Byte]): this.type = copyConstructor.newInstance(blob).asInstanceOf[this.type]
}

object ByteEncoded {

  implicit def fromBytes(bytes: Array[Byte]) = new ByteEncoded(bytes)
  implicit def toBytes(v: ByteEncoded) = v.asBytes

  implicit def fromKey(key: Key): ByteEncoded = {
    new ByteEncoded(key.getEncoded)
  }
}

//TODO: this is defective as new Base64Wrapper("AM28t0").asBase64Str = "AM28", same problem may happen to other impl, need to fix properly!
class Base64Wrapper(val blob: Array[Byte]) extends ByteEncoded(blob) {

  def this(str: String) {

    this(DatatypeConverter.parseBase64Binary(str))
  }

  def asBase64Str = toBase64Str

  def map(f: String => String) = new Base64Wrapper(f(this.asBase64Str)) // TODO: aggregate this
}

class Base16Wrapper(val blob: Array[Byte]) extends ByteEncoded(blob) {

  def this(str: String) {

    this(DatatypeConverter.parseHexBinary(str))
  }

  def asBase16Str = toBase16Str
  override def toString = asBase16Str
  def map(f: String => String) = new Base16Wrapper(f(this.asBase16Str))
}

class UTF8Wrapper(val blob: Array[Byte]) extends ByteEncoded(blob) {

  def this(str: String) {

    this(str.getBytes("UTF-8"))
  }

  def asUTF8Str = toUTF8Str
  override def toString: String = asUTF8Str
  def map(f: String => String): ByteEncoded = new UTF8Wrapper(f(this.asUTF8Str))
}
