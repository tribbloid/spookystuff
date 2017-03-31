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
                              formats: Formats
                            ) extends org.apache.spark.ml.param.Param[Obj](parent, name, doc, isValid) {

  /** Creates a param pair with the given value (for Java). */
  //    override def w(value: M): ParamPair[M] = super.w(value)

  override def jsonEncode(value: Obj): String = {

    outer.toMessageAPI(value)
      .compactJSON(outer.formats)
  }

  override def jsonDecode(json: String): Obj = {

    implicit val mf: Manifest[outer.M] = outer.mf

    val messageV: outer.M = outer.fromJSON(json)
    messageV match {
      case v: MessageRepr[_] =>
        v.toObject.asInstanceOf[Obj]
      case _ =>
        //TODO: this is hacking, is there a more rigorous impl?
        if (outer.isInstanceOf[MessageReader[Obj]]) {
          messageV.asInstanceOf[Obj]
        }
        else {
          throw new UnsupportedOperationException(
            s"jsonDecode is not implemented in ${outer.getClass.getName}[${outer.mf.runtimeClass.getName}]"
          )
        }
    }
  }
}
