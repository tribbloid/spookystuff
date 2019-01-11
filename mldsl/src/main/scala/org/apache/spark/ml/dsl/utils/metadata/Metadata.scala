package org.apache.spark.ml.dsl.utils.metadata

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

class Metadata(
    override val self: ListMap[String, Any] = ListMap.empty
) extends MetadataLike {}

object Metadata extends MetadataRelay[Metadata] {
  override def fromListMap(vs: ListMap[String, Any]): Metadata = new Metadata(vs)
}
