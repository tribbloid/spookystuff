package org.apache.spark.ml.dsl.utils

import javax.xml.bind.DatatypeConverter

/**
  * Created by peng on 31/10/16.
  */
abstract class EncodedStrWrapper {

  def blob: Array[Byte]
  def asBytes = blob

  lazy val toBase64Str = DatatypeConverter.printBase64Binary(blob)
  lazy val toBase16Str = DatatypeConverter.printHexBinary(blob)
  lazy val toUTF8Str = new String(blob, "UTF-8")

  def str: String
  override def toString = str

  def map(f: String => String): EncodedStrWrapper
}

case class Base64Wrapper(blob: Array[Byte]) extends EncodedStrWrapper {

  def this(str: String) {

    this(DatatypeConverter.parseBase64Binary(str))
  }

  def asBase64Str = toBase16Str
  override def str: String = asBase64Str
  def map(f: String => String) = new Base64Wrapper(f(this.str))
}

case class Base16Wrapper(blob: Array[Byte]) extends EncodedStrWrapper {

  def this(str: String) {

    this(DatatypeConverter.parseHexBinary(str))
  }

  def asBase16Str = toBase16Str
  override def str: String = asBase16Str
  def map(f: String => String) = new Base16Wrapper(f(this.str))
}

case class UTF8Wrapper(blob: Array[Byte]) extends EncodedStrWrapper {

  def this(str: String) {

    this(str.getBytes("UTF-8"))
  }

  def asUTF8Str = toUTF8Str
  override def str: String = asUTF8Str
  override def map(f: (String) => String): EncodedStrWrapper = new UTF8Wrapper(f(this.str))
}