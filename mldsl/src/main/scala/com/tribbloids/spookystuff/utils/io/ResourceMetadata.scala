package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.NOTSerializable
import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI, MetadataRelay}
import org.json4s.JsonAST.JValue

import scala.language.implicitConversions

case class ResourceMetadata(
                             uri: String,
                             name: Option[String] = None,
                             //if None, infer it from content.
                             declaredContentType: Option[String] = None,
                             misc: Map[String, JValue] = Map.empty,
                             children: Seq[ResourceMetadata] = Nil
                           ) extends MessageAPI {

  lazy val effectiveMetadata = MetadataRelay.M(misc).toObject
}

abstract class Resource[+T](
                             val v: T
                           ) extends NOTSerializable {

  def metadata: ResourceMetadata
}

object Resource {

  implicit def unbox[T](obj: Resource[T]): T = obj.v
}