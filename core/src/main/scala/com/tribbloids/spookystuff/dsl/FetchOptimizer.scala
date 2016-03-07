package com.tribbloids.spookystuff.dsl

/**
 * Created by peng on 1/27/15.
 */
sealed trait FetchOptimizer

object FetchOptimizers {
  //this won't merge identical traces and do lookup, only used in case each resolve may yield different result
  case object Narrow extends FetchOptimizer

  case object Wide extends FetchOptimizer

  //group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
  //reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
  case object WebCacheAware extends FetchOptimizer

  //case object Inductive extends QueryOptimizer
  //
  //case object AutoDetect extends QueryOptimizer
}