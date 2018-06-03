package org.apache.spark.ml.dsl.utils.metadata

trait ParamLike extends Serializable {
  val name: String = this.getClass.getSimpleName
}
object ParamLike {
}
