package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row._

//TODO: add options to flatten pages
case class FlattenPlan(
                        child: ExecutionPlan,
                        flattenField: Field,
                        ordinalField: Field,
                        sampler: Sampler[Any],
                        isLeft: Boolean
                              ) extends ExecutionPlan(
  child,
  schemaOpt = Some(child.schema ++ Option(ordinalField))
) {

  override def doExecute(): SquashedRowRDD = {
    child
      .rdd()
      .map(_.flattenData(flattenField, ordinalField, isLeft, sampler))
  }
}
