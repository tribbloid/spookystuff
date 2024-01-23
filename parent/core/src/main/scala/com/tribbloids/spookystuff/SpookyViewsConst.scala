package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.util.Caching.ConcurrentMap

object SpookyViewsConst {

  val SPARK_JOB_DESCRIPTION: String = "spark.job.description"
  val SPARK_JOB_GROUP_ID: String = "spark.jobGroup.id"
  val SPARK_JOB_INTERRUPT_ON_CANCEL: String = "spark.job.interruptOnCancel"
  val RDD_SCOPE_KEY: String = "spark.rdd.scope"
  val RDD_SCOPE_NO_OVERRIDE_KEY: String = "spark.rdd.scope.noOverride"

  // (stageID -> threadID) -> isExecuted
  val perCoreMark: ConcurrentMap[(Int, Long), Boolean] = ConcurrentMap()
  // stageID -> isExecuted
  val perWorkerMark: ConcurrentMap[Int, Boolean] = ConcurrentMap()

  // large enough such that all idle threads has a chance to pick up >1 partition
  val REPLICATING_FACTOR: Int = 16
}
