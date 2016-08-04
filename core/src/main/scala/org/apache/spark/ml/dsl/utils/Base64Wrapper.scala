package org.apache.spark.ml.dsl.utils

import javax.xml.bind.DatatypeConverter

/**
 * Created by peng on 4/5/15.
 */
//TODO: change main datum to byte array
//case class Base64Wrapper(str: String) {
//
//  def this(blob: Array[Byte]) {
//
//    this(java.util.Base64.getEncoder.encodeToString(blob))
//  }
//
//  override def toString = str
//
////  override def equals(obj: Any) = obj match {
////    case v: Base64StringWrapper if v.str == str => true
////    case _ => false
////  }
////
////  override def hashCode = str.hashCode
//
//  def map(f: String => String) = new Base64Wrapper(f(this.str))
//
//  def asBase64Str = str
//
//  def asBytes = java.util.Base64.getDecoder.decode(str)
//}

case class Base64Wrapper(str: String) {

  def this(blob: Array[Byte]) {

    this(DatatypeConverter.printBase64Binary(blob))
  }

  override def toString = str

  //  override def equals(obj: Any) = obj match {
  //    case v: Base64StringWrapper if v.str == str => true
  //    case _ => false
  //  }
  //
  //  override def hashCode = str.hashCode

  def map(f: String => String) = new Base64Wrapper(f(this.str))

  def asBase64Str = str

  def asBytes = DatatypeConverter.parseBase64Binary(str)
}