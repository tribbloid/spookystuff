package org.apache.spark.ml.dsl.utils

import org.json4s.Extraction._
import org.json4s.JsonAST.JString
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.reflect.{TypeInfo, _}

abstract class XMLWeakDeserializer[T: Manifest] extends Serializer[T] {

  // cannot serialize
  override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = PartialFunction.empty
}

// <tag>12</tag> => tag: 12
object StringToNumberDeserializer extends XMLWeakDeserializer[Any] {

  override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = Function.unlift{

    case (TypeInfo(cc, _), JString(v)) =>
      val parsed = cc match {
        case java.lang.Byte.TYPE => v.toByte
        case java.lang.Short.TYPE => v.toShort
        case java.lang.Character.TYPE => v.toInt.toChar
        case java.lang.Integer.TYPE => v.toInt
        case java.lang.Long.TYPE => v.toLong
        case java.lang.Float.TYPE => v.toFloat
        case java.lang.Double.TYPE => v.toDouble
        case java.lang.Boolean.TYPE => v.toBoolean
        case _ => null
        //TODO: add boxed type, or use try/errorToNone
      }
      Option(parsed)
    case _ => None
  }
}

// <tag/> => tag: {}
object EmptyStringToEmptyObjectDeserializer extends XMLWeakDeserializer[Any] {

  override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = Function.unlift{

    case (ti@ TypeInfo(cc, _), JString(str))
      if !cc.isAssignableFrom(classOf[String]) && str.trim.isEmpty =>
      try {
        Some(extract(JObject(), ti)(format))
      }
      catch {
        case e: Throwable =>
          None
      }

    case _ => None
  }
}

// <tag>12</tag> => tag: [12]
// <tag>abc</tag> => tag: ["abc"]
object ElementToArrayDeserializer extends XMLWeakDeserializer[Any] {

  val listClass = classOf[List[_]]
  val seqClass = classOf[Seq[_]]
  val setClass = classOf[Set[_]]
  val arrayListClass = classOf[java.util.ArrayList[_]]

  override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = {

    case (ti@ TypeInfo(this.listClass | this.seqClass, _), jv) if !jv.isInstanceOf[JArray] =>
      extractInner(ti, jv, format).toList

    case (ti@ TypeInfo(this.setClass, _), jv) if !jv.isInstanceOf[JArray] =>
      extractInner(ti, jv, format).toSet

    case (ti@ TypeInfo(this.arrayListClass, _), jv) if !jv.isInstanceOf[JArray] =>
      import scala.collection.JavaConverters._

      new java.util.ArrayList[Any](extractInner(ti, jv, format).toList.asJava)

    case (ti@ TypeInfo(cc, _), jv) if !jv.isInstanceOf[JArray] && cc.isArray =>
      val a = extractInner(ti, jv, format).toArray
      mkTypedArray(a, firstTypeArg(ti))
  }

  def mkTypedArray(a: Array[_], typeArg: ScalaType) = {
    import java.lang.reflect.Array.{newInstance => newArray}

    a.foldLeft((newArray(typeArg.erasure, a.length), 0)) { (tuple, e) => {
      java.lang.reflect.Array.set(tuple._1, tuple._2, e)
      (tuple._1, tuple._2 + 1)
    }}._1
  }

  def extractInner(ti: TypeInfo, jv: JValue, format: Formats): Option[Any] = {
    try {
      val result = jv match {
        case JNothing => None
        case _ => Some(extract(jv, firstTypeArg(ti))(format))
      }
      result
    }
    catch {
      case e: Exception =>

        val details = Seq(
          ">>> Extract JSON >>>",
          pretty(render(jv))
        ) ++
        Seq(
          ">>> First Type : " + firstTypeArg(ti)
        ) ++
          Seq("------ customSerializers ------") ++
          format.customSerializers ++
          Seq("------ fieldSerializers -------") ++
          format.fieldSerializers ++
          Seq("--------- primitives ----------") ++
          format.primitives
        throw new MappingException(
          "",
          e
        ) with Detail {
          override def detail: String = details.mkString("\n")

          override def toString = toStringVerbose
        }
    }
  }

  def firstTypeArg(ti: TypeInfo): ScalaType = {
    val tpe: ScalaType = ScalaType.apply(ti)
    val firstTypeArg = tpe.typeArgs.head
    firstTypeArg
  }
}