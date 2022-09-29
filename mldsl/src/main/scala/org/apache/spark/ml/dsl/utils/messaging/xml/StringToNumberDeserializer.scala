package org.apache.spark.ml.dsl.utils.messaging.xml

import org.json4s.JsonAST.JString
import org.json4s._
import org.json4s.reflect.TypeInfo

// <tag>12</tag> => tag: 12
object StringToNumberDeserializer extends XMLWeakDeserializer[Any] {

  override def _deserialize(
      implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), Any] = Function.unlift {

    case (ti @ TypeInfo(cc, _), JString(v)) =>
      val parsed = cc match {
        case java.lang.Byte.TYPE      => v.toByte
        case java.lang.Short.TYPE     => v.toShort
        case java.lang.Character.TYPE => v.toInt.toChar
        case java.lang.Integer.TYPE   => v.toInt
        case java.lang.Long.TYPE      => v.toLong
        case java.lang.Float.TYPE     => v.toFloat
        case java.lang.Double.TYPE    => v.toDouble
        case java.lang.Boolean.TYPE   => v.toBoolean
        case _                        => null
        // TODO: add boxed type, or use try/errorToNone
      }
      Option(parsed)
    case _ => None
  }
}
