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

  override def doExecute(): BottleneckRDD = {
    val grouped = super.unsquashedRDD
      .keyBy { row =>
        exprs.map(expr => expr.apply(row))
      }
      .mapValues(v => Vector(v.dataRow))

    grouped
      .reduceByKey(reducer)
      .map(v => BottleneckRow(v._2))
  }
}
