package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row._

/**
  * discard pages
  */
case class AggregatePlan(
    override val child: ExecutionPlan,
    exprs: Seq[(FetchedRow => Any)],
    reducer: RowReducer
) extends UnaryPlan(child) {

  override def doExecute(): SquashedFetchedRDD = {
    val keyedRDD = super.unsquashedRDD
      .keyBy { row =>
        exprs.map(expr => expr.apply(row))
      }
      .mapValues(v => Iterable(v.dataRow))

    keyedRDD
      .reduceByKey(reducer)
      .map(v => SquashedFetchedRow(v._2.toArray))
  }
}
