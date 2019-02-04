package org.apache.spark.ml.dsl.utils.metadata

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

//TODO: superceded to Spark SQL metadata?
class Metadata(
    override val self: ListMap[String, Any] = ListMap.empty
) extends MetadataLike {

  override def toString: String = self.toString
}

object Metadata extends MetadataRelay[Metadata] {
  override def fromListMap(vs: ListMap[String, Any]): Metadata = new Metadata(vs)
}
