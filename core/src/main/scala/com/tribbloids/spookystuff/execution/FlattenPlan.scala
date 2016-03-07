package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row._

//TODO: add options to flatten pages
case class FlattenPlan(
                        child: AbstractExecutionPlan,
                        flattenField: Field,
                        ordinalField: Field,
                        sampler: Sampler[Any],
                        isLeft: Boolean
                              ) extends AbstractExecutionPlan(
  child,
  schemaOpt = Some(child.fieldSet ++ Option(ordinalField))
) {

  override def doExecute(): SquashedRowRDD = {
    child
      .rdd()
      .map(_.flattenData(flattenField, ordinalField, isLeft, sampler))
  }
}
