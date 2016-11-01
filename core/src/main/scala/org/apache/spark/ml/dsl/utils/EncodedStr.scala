package org.apache.spark.ml.dsl.utils

import javax.xml.bind.DatatypeConverter

/**
  * Created by peng on 31/10/16.
  */
abstract class EncodedStr {

  def asBytes: Array[Byte]

  def str: String
  override def toString = str

  def map(f: String => String): EncodedStr
}

case class Base64Wrapper(str: String) extends EncodedStr {

  def this(blob: Array[Byte]) {

    this(DatatypeConverter.printBase64Binary(blob))
  }

  def map(f: String => String) = new Base64Wrapper(f(this.str))

  def asBase64Str = str

  def asBytes = DatatypeConverter.parseBase64Binary(str)
}

//TODO: need test
//TODO: change main datum to byte array
case class Base16Wrapper(str: String) extends EncodedStr {

  def this(blob: Array[Byte]) {

    this(DatatypeConverter.printHexBinary(blob))
  }

  def map(f: String => String) = new Base16Wrapper(f(this.str))

  def asBase16Str = str

  def asBytes = DatatypeConverter.parseHexBinary(str)
}

//TODO: why so many boilerplates?
case class UTF8Wrapper(override val str: String) extends EncodedStr {

  def this(blob: Array[Byte]) {

    this(new String(blob, "UTF-8"))
  }

  override def asBytes: Array[Byte] = str.getBytes("UTF-8")

  override def map(f: (String) => String): EncodedStr = new UTF8Wrapper(f(this.str))
}