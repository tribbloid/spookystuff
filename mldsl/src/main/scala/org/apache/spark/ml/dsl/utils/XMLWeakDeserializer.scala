package org.apache.spark.ml.dsl.utils

import java.util

import com.tribbloids.spookystuff.utils.CachingUtils
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentCache
import org.apache.spark.ml.dsl.utils.messaging.MessageAPI
import org.json4s.Extraction._
import org.json4s.JsonAST.JString
import org.json4s._
import org.json4s.reflect.{TypeInfo, _}

object XMLWeakDeserializer {
  case class ExceptionMetadata(
      jValue: Option[JValue] = None,
      typeInfo: Option[String] = None,
      serDe: SerDeMetadata
  ) extends MessageAPI

  case class SerDeMetadata(
      reporting: Option[String] = None,
      primitives: Seq[String] = Nil,
      field: Map[String, String] = Map.empty,
      custom: Seq[String] = Nil
  )

  val cached: ConcurrentCache[Long, ParsingException] = CachingUtils.ConcurrentCache[Long, ParsingException]()

  trait ExceptionLike extends Throwable with Verbose {

//    def metadata: ExceptionMetadata

    override def getMessage: String = detailedStr
  }

  class ParsingException(
      override val shortStr: String,
      cause: Exception,
      val metadata: ExceptionMetadata
  ) extends MappingException(shortStr, cause)
      with ExceptionLike {

    {
      cached.put(System.currentTimeMillis(), this)
    }

    override def detail: String =
      s"""
        |"METADATA": ${metadata.toJSON()}
        |""".trim.stripMargin
  }

  case class UnrecoverableError(
      override val shortStr: String,
      cause: Throwable
//      override val metadata: ExceptionMetadata
  ) extends Error
      with ExceptionLike {

    override def detail: String =
      s"""
         |### [RECENT XML EXCEPTIONS] ###
         |
         |${cached.toSeq.sortBy(_._1).map(_._2).mkString("\n")}
         |""".stripMargin
  }
}

abstract class XMLWeakDeserializer[T: Manifest] extends Serializer[T] {

  import XMLWeakDeserializer._

  // cannot serialize
  override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = PartialFunction.empty

  def exceptionMetadata(
      jValue: JValue,
      typeInfo: TypeInfo,
      formats: Formats
  ): ExceptionMetadata = ExceptionMetadata(
    Some(jValue),
    Some(typeInfo.toString),
    SerDeMetadata(
      Some(this.getClass.getName),
      formats.primitives.toSeq.map(_.toString),
      Map(formats.fieldSerializers.map(v => v._1.getName -> v._2.toString): _*),
      formats.customSerializers.map(_.toString)
    )
  )

  def wrapException[A](ti: TypeInfo, jv: JValue, format: Formats)(fn: => A): A = {

    lazy val metadata = exceptionMetadata(jv, ti, format)

    try {
      fn
    } catch {
      case e: MappingException =>
        throw new ParsingException(
          e.getMessage,
          e,
          metadata
        )

      case e: Exception =>
//        throw new ParsingException(
//          e.getMessage,
//          e,
//          metadata
//        )
        throw e

      case e: Throwable =>
        throw UnrecoverableError(
          e.getClass.getSimpleName,
          e
        )
    }
  }

  override final def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), T] = {
    val result: ((TypeInfo, JValue)) => Option[T] = {
      case (ti, jv) =>
        wrapException(ti, jv, format) {
          _deserialize(format).lift.apply(ti -> jv)
        }
    }
    Function.unlift(result)
  }

  def _deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), T]
}

// <tag>12</tag> => tag: 12
object StringToNumberDeserializer extends XMLWeakDeserializer[Any] {

  override def _deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = Function.unlift {

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
        //TODO: add boxed type, or use try/errorToNone
      }
      Option(parsed)
    case _ => None
  }
}

// <tag/> => tag: {}
object EmptyStringToEmptyObjectDeserializer extends XMLWeakDeserializer[Any] {

  override def _deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = Function.unlift {

    case (ti @ TypeInfo(cc, _), jv @ JString(str)) if !cc.isAssignableFrom(classOf[String]) && str.trim.isEmpty =>
//      wrapException(ti, jv, format) {
      Some(extract(JObject(), ti)(format))
//      }

    case _ => None
  }
}

// <tag>12</tag> => tag: [12]
// <tag>abc</tag> => tag: ["abc"]
object ElementToArrayDeserializer extends XMLWeakDeserializer[Any] {

  val listClass: Class[List[_]] = classOf[List[_]]
  val seqClass: Class[Seq[_]] = classOf[Seq[_]]
  val setClass: Class[Set[_]] = classOf[Set[_]]
  val arrayListClass: Class[util.ArrayList[_]] = classOf[java.util.ArrayList[_]]

  override def _deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = {

    case (ti @ TypeInfo(this.listClass | this.seqClass, _), jv) if !jv.isInstanceOf[JArray] =>
      extractInner(ti, jv, format).toList

    case (ti @ TypeInfo(this.setClass, _), jv) if !jv.isInstanceOf[JArray] =>
      extractInner(ti, jv, format).toSet

    case (ti @ TypeInfo(this.arrayListClass, _), jv) if !jv.isInstanceOf[JArray] =>
      import scala.collection.JavaConverters._

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
      }
      ._1
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
