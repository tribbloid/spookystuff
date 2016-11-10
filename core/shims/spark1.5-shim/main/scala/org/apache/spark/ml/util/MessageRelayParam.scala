package org.apache.spark.ml.dsl.utils

import org.apache.spark.annotation.DeveloperApi
import org.json4s._

import scala.language.implicitConversions

/**
  * :: DeveloperApi ::
  * ML Param only supports string & vectors, this class extends support to all objects
  */
@DeveloperApi
class MessageRelayParam[Obj](
                              outer: MessageRelay[Obj],
                              parent: String,
                              name: String,
                              doc: String,
                              isValid: Obj => Boolean,
                              // serializer = SparkEnv.get.serializer
                              formats: Formats
                            ) extends org.apache.spark.ml.param.Param[Obj](parent, name, doc, isValid) {


  /** Creates a param pair with the given value (for Java). */
  //    override def w(value: M): ParamPair[M] = super.w(value)

  def jsonEncode(value: Obj): String = {

    outer.toMessage(value)
      .compactJSON(outer.formats)
  }

  def jsonDecode(json: String): Obj = {

    val message: outer.M = outer.fromJSON(json)
    message match {
      case v: MessageRepr[_] =>
        v.toObject.asInstanceOf[Obj]
      case _ =>
        throw new UnsupportedOperationException("jsonDecode is not implemented")
    }
  }
}
