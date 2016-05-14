package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.row._

/**
  * extract parts of each Page and insert into their respective context
  * if a key already exist in old context it will be replaced with the new one.
  *
  * @return new PageRowRDD
  */
case class ExtractPlan(
                        child: ExecutionPlan,
                        exprs: Seq[NamedExpr[Any]]
                      ) extends ExecutionPlan(
  child,
  schemaOpt = {
    val putFields: Seq[Field] = exprs.map {
      expr =>
        expr.field
    }
    putFields.groupBy(identity).foreach{
      v =>
        if (v._2.size > 1) throw new QueryException(s"Field ${v._1.name} already exist")
    }

    Some(child.schema ++ putFields)
  }) {

  override def doExecute(): SquashedRowRDD = {

    child
      .rdd(true)
      .map(_.extract(exprs: _*))
  }
}