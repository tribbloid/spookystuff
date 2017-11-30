package org.apache.spark.ml.dsl

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by peng on 11/04/16.
  */
object PipelineModelShim {

  def create(
              stages: Array[Transformer],
              uid: String = Identifiable.randomUID(classOf[PipelineModel].getSimpleName)
            ) = new PipelineModel(uid, stages)

  def apply(stages: Transformer*) = create(stages.toArray)
}
