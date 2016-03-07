package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{Field, SquashedRowRDD}

/**
  * Created by peng on 27/03/16.
  */
case class RemovePlan(
                       child: AbstractExecutionPlan,
                       fields: Seq[Field]
                     ) extends AbstractExecutionPlan(
  child,
  schemaOpt = Some(child.fieldSet -- fields)
) {

  override def doExecute(): SquashedRowRDD = {
    child.rdd().map(_.remove(fields: _*))
  }
}
