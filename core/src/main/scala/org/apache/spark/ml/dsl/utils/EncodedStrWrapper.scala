package org.apache.spark.ml.dsl.utils

import javax.xml.bind.DatatypeConverter

import com.tribbloids.spookystuff.utils.IDMixin

/**
  * Created by peng on 31/10/16.
  */
abstract class EncodedStrWrapper extends IDMixin {

  def blob: Array[Byte]
  def asBytes = blob

  lazy val toBase64Str = DatatypeConverter.printBase64Binary(blob)
  lazy val toBase16Str = DatatypeConverter.printHexBinary(blob)
  lazy val toUTF8Str = new String(blob, "UTF-8")

  def str: String
  def _id = str
  override def toString = str

  def map(f: String => String): EncodedStrWrapper
}

class Base64Wrapper(val blob: Array[Byte]) extends EncodedStrWrapper {

  def this(str: String) {

    this(DatatypeConverter.parseBase64Binary(str))
  }

  def asBase64Str = toBase64Str
  override def str: String = asBase64Str
  def map(f: String => String) = new Base64Wrapper(f(this.str))
}

class Base16Wrapper(val blob: Array[Byte]) extends EncodedStrWrapper {

  def this(str: String) {

    this(DatatypeConverter.parseHexBinary(str))
  }

  def asBase16Str = toBase16Str
  override def str: String = asBase16Str
  def map(f: String => String) = new Base16Wrapper(f(this.str))
}

class UTF8Wrapper(val blob: Array[Byte]) extends EncodedStrWrapper {

  def this(str: String) {

    this(str.getBytes("UTF-8"))
  }

  def asUTF8Str = toUTF8Str
  override def str: String = asUTF8Str
  override def map(f: (String) => String): EncodedStrWrapper = new UTF8Wrapper(f(this.str))
}