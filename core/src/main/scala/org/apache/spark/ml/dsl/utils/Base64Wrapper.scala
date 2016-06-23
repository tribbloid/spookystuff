package org.apache.spark.ml.dsl.utils

import java.util.Base64

/**
 * Created by peng on 4/5/15.
 */
//TODO: change main datum to byte array
case class Base64Wrapper(str: String) {

  def this(blob: Array[Byte]) {

    this(Base64.getEncoder.encodeToString(blob))
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

  def asBytes = Base64.getDecoder.decode(str)
}