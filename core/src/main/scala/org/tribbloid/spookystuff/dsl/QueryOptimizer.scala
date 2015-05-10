package org.tribbloid.spookystuff.dsl

/**
 * Created by peng on 1/27/15.
 */
sealed abstract class QueryOptimizer

//this won't merge identical traces and do lookup, only used in case each resolve may yield different result
case object Narrow extends QueryOptimizer

case object Wide extends QueryOptimizer

//group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
//reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
case object WideLookup extends QueryOptimizer

//case object Inductive extends QueryOptimizer
//
//case object AutoDetect extends QueryOptimizer