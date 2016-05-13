package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row._

/**
  * discard pages
  */
case class AggPlan(
                    child: ExecutionPlan,
                    exprs: Seq[(PageRow => Any)],
                    reducer: RowReducer
                  ) extends ExecutionPlan(child) {

  override def doExecute(): SquashedRowRDD = {
    val keyedRDD = super.unsquashedRDD
      .keyBy {
        row =>
          exprs.map(expr => expr.apply(row))
      }
      .mapValues(v => Iterable(v._1))

    keyedRDD
      .reduceByKey(reducer)
      .map(v => SquashedPageRow(v._2.toArray))
  }
}