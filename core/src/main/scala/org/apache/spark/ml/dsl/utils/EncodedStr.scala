package org.apache.spark.ml.dsl.utils

import javax.xml.bind.DatatypeConverter

/**
  * Created by peng on 31/10/16.
  */
abstract class EncodedStr {

  def blob: Array[Byte]
  def asBytes = blob

  def asBase64Str = DatatypeConverter.printBase64Binary(blob)
  def asBase16Str = DatatypeConverter.printHexBinary(blob)
  def asUTF8Str = new String(blob, "UTF-8")

  def str: String
  override def toString = str

  def map(f: String => String): EncodedStr
}

case class Base64Wrapper(blob: Array[Byte]) extends EncodedStr {

  def this(str: String) {

    this(DatatypeConverter.parseBase64Binary(str))
  }

  override def str: String = asBase64Str
  def map(f: String => String) = new Base64Wrapper(f(this.str))
}

case class Base16Wrapper(blob: Array[Byte]) extends EncodedStr {

  def this(str: String) {

    this(DatatypeConverter.parseHexBinary(str))
  }

  override def str: String = asBase16Str
  def map(f: String => String) = new Base16Wrapper(f(this.str))
}

case class UTF8Wrapper(blob: Array[Byte]) extends EncodedStr {

  def this(str: String) {

    this(str.getBytes("UTF-8"))
  }

  override def str: String = asUTF8Str
  override def map(f: (String) => String): EncodedStr = new UTF8Wrapper(f(this.str))
}