package org.apache.spark.ml.dsl.utils

import javax.xml.bind.DatatypeConverter

/**
 * Created by peng on 4/5/15.
 */
//TODO: need test
//TODO: change main datum to byte array
case class Base16Wrapper(str: String) {

  def this(blob: Array[Byte]) {

    this(DatatypeConverter.printHexBinary(blob))
  }

  override def toString = str

//  override def equals(obj: Any) = obj match {
//    case v: Base16StringWrapper if v.str == str => true
//    case _ => false
//  }
//
//  override def hashCode = str.hashCode

  def map(f: String => String) = new Base16Wrapper(f(this.str))

  def asBase16Str = str

  def asBytes = DatatypeConverter.parseHexBinary(str)
}