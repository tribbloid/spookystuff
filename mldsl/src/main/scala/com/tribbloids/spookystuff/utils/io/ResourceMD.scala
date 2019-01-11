package com.tribbloids.spookystuff.utils.io

import org.apache.spark.ml.dsl.utils.metadata.{Metadata, MetadataRelay}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

class ResourceMD(
    override val self: ListMap[String, Any] = ListMap.empty
) extends Metadata(self) {

  object `uri` extends Param[String]
  object `name` extends Param[String]
  object `type` extends Param[String]
  object `content-type` extends Param[String]
  object `length` extends Param[Long]
  object `status-code` extends Param[Int]

  object `isDir` extends Param[Boolean]
}

object ResourceMD extends MetadataRelay[ResourceMD] {
  override def fromListMap(vs: ListMap[String, Any]): ResourceMD = new ResourceMD(vs)
}
