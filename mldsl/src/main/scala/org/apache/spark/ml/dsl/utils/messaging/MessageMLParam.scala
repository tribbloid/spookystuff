package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.annotation.DeveloperApi
import org.json4s._

/**
  * :: DeveloperApi :: ML Param only supports string & vectors, this class extends support to all objects
  */
@DeveloperApi
class MessageMLParam[Obj](
    outer: Codec[Obj],
    parent: String,
    name: String,
    doc: String,
    isValid: Obj => Boolean,
    formats: Formats
) extends org.apache.spark.ml.param.Param[Obj](parent, name, doc, isValid) {

  import outer._

  /** Creates a param pair with the given value (for Java). */
  //    override def w(value: M): ParamPair[M] = super.w(value)

  override def jsonEncode(value: Obj): String = {

    value.compactJSON(outer.formats)
  }

  override def jsonDecode(json: String): Obj = {

    outer.fromJSON(json)

    //    implicit val mf: Manifest[outer.M] = outer.mf

    //    val messageV: outer.M = outer.fromJSON(json)
    //    messageV match {
    //      case v: MessageAPI_<=>[_] =>
    //        v.<<<.asInstanceOf[Obj]
    //      case _ =>
    //        //TODO: this is hacking, is there a more rigorous impl?
    //        if (outer.isInstanceOf[MessageReader[Obj]]) {
    //          messageV.asInstanceOf[Obj]
    //        }
    //        else {
    //        }
    //    }
  }
}
