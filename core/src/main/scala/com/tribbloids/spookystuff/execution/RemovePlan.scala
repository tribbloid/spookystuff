package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{Field, SquashedFetchedRDD}

/**
  * Created by peng on 27/03/16.
  */
case class RemovePlan(
                       child: ExecutionPlan,
                       toBeRemoved: Seq[Field]
                     ) extends ExecutionPlan(
  child,
  schemaOpt = Some(child.fields -- toBeRemoved)
) {

  override def doExecute(): SquashedFetchedRDD = {
    child.rdd().map(_.remove(toBeRemoved: _*))
  }
}
