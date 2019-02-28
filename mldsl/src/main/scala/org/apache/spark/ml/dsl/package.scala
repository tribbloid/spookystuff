package org.apache.spark.ml

import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}

import scala.collection.immutable.ListMap

/**
  * Created by peng on 10/04/16.
  */
package object dsl {

  type NamedStage = AbstractNamedStage[PipelineStage]
  val NamedStage = AbstractNamedStage

  type StepMap[A, B] = ListMap[A, B]
  val StepMap = ListMap

  type StepBuffer[A, B] = scala.collection.mutable.LinkedHashMap[A, B]
  val StepBuffer = scala.collection.mutable.LinkedHashMap

  type MultiPartCompaction[V] = Set[Seq[V]] => Map[Seq[V], Seq[V]]

  type PathCompaction = MultiPartCompaction[String]

}
