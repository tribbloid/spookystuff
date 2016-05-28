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
  schemaOpt = Some(child.fields ++ Option(ordinalField))
) {

  override def doExecute(): SquashedFetchedRDD = {
    child
      .rdd()
      .map(_.flattenData(flattenField, ordinalField, isLeft, sampler))
  }
}
