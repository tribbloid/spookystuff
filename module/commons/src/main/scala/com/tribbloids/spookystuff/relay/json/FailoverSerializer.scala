package com.tribbloids.spookystuff.relay.json

import org.json4s.{Formats, JValue, RichSerializer}

import scala.util.Try

case class FailoverSerializer(
    delegate: RichSerializer[Any]
) extends RichSerializer[Any] {
  // will only be used if default serializer cannot handle this type

  import org.json4s.reflect.*

  override def deserialize(
      implicit
      format: Formats
  ): PartialFunction[(ScalaType, JValue), Any] = { ??? }

  override def serialize(
      implicit
      formats: Formats
  ): PartialFunction[Any, JValue] = { v =>
    val reflTry = Try {
      Reflector.describe(Manifest.classType(v.getClass))
    }
    reflTry match {
      case scala.util.Success(_) =>
        throw new MatchError("b") // no need to use this, delegate to primary json4s
      case scala.util.Failure(_) =>
        delegate.serialize.apply(v)
    }
  }
}

object FailoverSerializer {
  import org.json4s.reflect.*

  case class GlobalCache(
      formats: Formats
  ) {

    val cache: Map[ScalaType, Boolean] = Map.empty

    def validateType(v: ScalaType): Boolean = {

      val reflTry = Try {
        Reflector.describeWithFormats(v)(formats)
      }
      ???
    }
  }

  lazy val indexed: Map[
    Formats,
    GlobalCache
  ] = Map.empty // for each type, true if need failover, false if not
  // determined exactly once per JVM
  // determined by:
  // 1. Reflector.describe
  // 2. given an instance, run 1 roundtrip test given current formats
  // this assumes that failures happen consistently for each type, but json4s impl may be broken
}
