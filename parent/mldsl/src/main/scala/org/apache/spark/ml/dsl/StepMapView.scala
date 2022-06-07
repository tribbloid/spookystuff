package org.apache.spark.ml.dsl

class StepMapView(val coll: StepMap[String, StepLike]) extends StepGraph {

  // generate new copy for each PipelineStage in this collection to
  override def replicate(suffix: String = ""): StepMapView = new StepMapView(
    coll = this.replicateColl(suffix = suffix)
  )
}
