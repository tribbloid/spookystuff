package com.tribbloids.spookystuff.relay.xml

import ai.acyclic.prover.commons.util.Caching
import com.tribbloids.spookystuff.relay.MessageAPI
import com.tribbloids.spookystuff.utils.Verbose
import org.json4s._
import org.json4s.reflect.TypeInfo

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

  private val cached = Caching.ConcurrentCache[Long, ParsingException]()

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
  override def serialize(
      implicit
      format: Formats
  ): PartialFunction[Any, JValue] = PartialFunction.empty

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
        throw e

      case e: Throwable =>
        throw UnrecoverableError(
          e.getClass.getSimpleName,
          e
        )
    }
  }

  final override def deserialize(
      implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), T] = {
    val result: ((TypeInfo, JValue)) => Option[T] = {
      case (ti, jv) =>
        wrapException(ti, jv, format) {
          _deserialize(format).lift.apply(ti -> jv)
        }
    }
    Function.unlift(result)
  }

  def _deserialize(
      implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), T]
}
