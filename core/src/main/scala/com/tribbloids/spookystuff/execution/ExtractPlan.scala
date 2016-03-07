package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.row._

/**
  * extract parts of each Page and insert into their respective context
  * if a key already exist in old context it will be replaced with the new one.
  *
  * @return new PageRowRDD
  */
case class ExtractPlan(
                        child: AbstractExecutionPlan,
                        exprs: Seq[Expression[Any]]
                      ) extends AbstractExecutionPlan(
  child,
  schemaOpt = {
    val putFields: Seq[Field] = exprs.map {
      expr =>
        expr.field
    }

    Some(child.fieldSet ++ putFields)
  }) {

  override def doExecute(): SquashedRowRDD = {

    child
      .rdd(true)
      .map(_.extract(exprs: _*))
  }
}