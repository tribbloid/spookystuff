package org.apache.spark.ml

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
  * Created by peng on 10/04/16.
  */
package object dsl {

  type NamedStage = AbstractNamedStage[PipelineStage]
  val NamedStage: AbstractNamedStage.type = AbstractNamedStage

  type StepMap[A, B] = ListMap[A, B]
  val StepMap: ListMap.type = ListMap

  type StepBuffer[A, B] = scala.collection.mutable.LinkedHashMap[A, B]
  val StepBuffer: mutable.LinkedHashMap.type = scala.collection.mutable.LinkedHashMap

  type MultiPartCompaction[V] = Set[Seq[V]] => Map[Seq[V], Seq[V]]

  type PathCompaction = MultiPartCompaction[String]

}
