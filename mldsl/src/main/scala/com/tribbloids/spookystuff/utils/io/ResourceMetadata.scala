package com.tribbloids.spookystuff.utils.io

import org.apache.spark.ml.dsl.utils.metadata.{Params, ParamsRelay}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

class ResourceMetadata(
    override val self: ListMap[String, Any] = ListMap.empty
) extends Params(self) {

  object `uri` extends Param[String]
  object `name` extends Param[String]
  object `type` extends Param[String]
  object `content-type` extends Param[String]
  object `length` extends Param[Long]
  object `status-code` extends Param[Int]

  object `isDir` extends Param[Boolean]
}

object ResourceMetadata extends ParamsRelay[ResourceMetadata] {
  override def fromListMap(vs: ListMap[String, Any]): ResourceMetadata = new ResourceMetadata(vs)
}
