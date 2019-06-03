package org.apache.spark.ml.dsl.utils.metadata

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

//TODO: superceded to Spark SQL metadata?
class Params(
    override val self: ListMap[String, Any] = ListMap.empty
) extends ParamsLike {

  override def toString: String = self.toString
}

object Params extends ParamsRelay[Params] {
  override def fromListMap(vs: ListMap[String, Any]): Params = new Params(vs)
}
