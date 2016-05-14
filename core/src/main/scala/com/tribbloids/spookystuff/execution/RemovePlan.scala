package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{Field, SquashedFetchedRDD}

/**
  * Created by peng on 27/03/16.
  */
case class RemovePlan(
                       child: ExecutionPlan,
                       fields: Seq[Field]
                     ) extends ExecutionPlan(
  child,
  schemaOpt = Some(child.schema -- fields)
) {

  override def doExecute(): SquashedFetchedRDD = {
    child.rdd().map(_.remove(fields: _*))
  }
}
