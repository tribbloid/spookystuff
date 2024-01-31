package com.tribbloids.spookystuff.relay.xml

import org.json4s.Extraction._
import org.json4s._
import org.json4s.reflect.{TypeInfo, _}

import java.util

// <tag>12</tag> => tag: [12]
// <tag>abc</tag> => tag: ["abc"]
object ElementToArrayDeserializer extends XMLWeakDeserializer[Any] {

  val listClass: Class[List[_]] = classOf[List[_]]
  val seqClass: Class[Seq[_]] = classOf[Seq[_]]
  val setClass: Class[Set[_]] = classOf[Set[_]]
  val arrayListClass: Class[util.ArrayList[_]] = classOf[java.util.ArrayList[_]]

  override def _deserialize(
      implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), Any] = {

    case (ti @ TypeInfo(this.listClass | this.seqClass, _), jv) if !jv.isInstanceOf[JArray] =>
      extractInner(ti, jv, format).toList

    case (ti @ TypeInfo(this.setClass, _), jv) if !jv.isInstanceOf[JArray] =>
      extractInner(ti, jv, format).toSet

    case (ti @ TypeInfo(this.arrayListClass, _), jv) if !jv.isInstanceOf[JArray] =>
      import scala.jdk.CollectionConverters._

      new java.util.ArrayList[Any](extractInner(ti, jv, format).toList.asJava)

    case (ti @ TypeInfo(cc, _), jv) if !jv.isInstanceOf[JArray] && cc.isArray =>
      val a = extractInner(ti, jv, format).toArray
      mkTypedArray(a, firstTypeArg(ti))
  }

  def mkTypedArray(a: Array[_], typeArg: ScalaType): AnyRef = {
    import java.lang.reflect.Array.{newInstance => newArray}

    a.foldLeft((newArray(typeArg.erasure, a.length), 0)) { (tuple, e) =>
      {
        java.lang.reflect.Array.set(tuple._1, tuple._2, e)
        (tuple._1, tuple._2 + 1)
      }
    }._1
  }

  def extractInner(ti: TypeInfo, jv: JValue, format: Formats): Option[Any] = {
    //    wrapException(ti, jv, format) {
    val result = jv match {
      case JNothing => None
      case _        => Some(extract(jv, firstTypeArg(ti))(format))
    }
    result
    //    }
  }

  def firstTypeArg(ti: TypeInfo): ScalaType = {
    val tpe: ScalaType = ScalaType.apply(ti)
    val firstTypeArg = tpe.typeArgs.head
    firstTypeArg
  }
}
