//package com.tribbloids.spookystuff.relay
//
//import org.apache.spark.annotation.DeveloperApi
//
///**
//  * :: DeveloperApi :: ML Param only supports string & vectors, this class extends support to all objects
//  */
//@DeveloperApi
//class MessageMLParam[Obj](
//    outer: Relay[Obj]#DecoderView,
//    parent: String,
//    name: String,
//    doc: String,
//    isValid: Obj => Boolean
//) extends org.apache.spark.ml.param.Param[Obj](parent, name, doc, isValid) {
//
//  override def jsonEncode(value: Obj): String = {
//
//    outer._outer.toEncoder_>>(value).compactJSON()
//  }
//
//  override def jsonDecode(json: String): Obj = {
//
//    outer.fromJSON(json)
//  }
//}
