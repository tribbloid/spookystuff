package com.tribbloids.spookystuff.relay.xml

import org.json4s.Extraction._
import org.json4s.JsonAST.JString
import org.json4s._
import org.json4s.reflect.TypeInfo

// <tag/> => tag: {}
object EmptyStringToEmptyObjectDeserializer extends XMLWeakDeserializer[Any] {

  override def _deserialize(
      implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), Any] = Function.unlift {

    case (ti @ TypeInfo(cc, _), JString(str)) if !cc.isAssignableFrom(classOf[String]) && str.trim.isEmpty =>
      //      wrapException(ti, jv, format) {
      Some(extract(JObject(), ti)(format))
    //      }

    case _ => None
  }
}
