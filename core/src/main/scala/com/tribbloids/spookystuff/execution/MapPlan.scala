package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.extractors.{Extractor, GetExpr, Resolved}
import com.tribbloids.spookystuff.row._
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}

trait MapPlan extends UnaryPlan {

  val fn: SquashedFetchedRow => SquashedFetchedRow

  final override def doExecute(): SquashedFetchedRDD = {

    child
      .rdd()
      .map(fn)
  }
}

/**
  * extract parts of each Page and insert into their respective context
  * if a key already exist in old context it will be replaced with the new one.
  *
  * @return new PageRowRDD
  */
case class ExtractPlan[+T](
                            override val child: ExecutionPlan,
                            exs: Seq[Extractor[T]]
                          ) extends UnaryPlan(child) with MapPlan {

  val resolver = child.schema.newResolver

  val _exs: Seq[Resolved[Any]] = resolver.resolve[T](exs: _*)

  override val schema = resolver.build

  override val fn: SquashedFetchedRow => SquashedFetchedRow = _.extract(_exs: _*)
}

case class FlattenPlan(
                        override val child: ExecutionPlan,
                        onField: Field,
                        ordinalField: Field,
                        sampler: Sampler[Any],
                        isLeft: Boolean
                      ) extends UnaryPlan(child) with MapPlan {

  val resolver = child.schema.newResolver

  val _on: TypedField = {
    val flattenType = GetExpr(onField).applyType(child.schema) match {
      case ArrayType(boxed, _) => boxed
      case v: DataType => v
    }
    val tf = TypedField(onField.!!, flattenType)

    resolver.resolveTyped(tf).head
  }

  val effectiveOrdinalField = Option(ordinalField) match {
    case Some(ff) =>
      ff.copy(isOrdinal = true)
    case None =>
      Field(_on.self.name + "_ordinal", isWeak = true, isOrdinal = true)
  }

  val _ordinal: TypedField = resolver.resolveTyped(TypedField(effectiveOrdinalField, ArrayType(IntegerType))).head

  override val schema = resolver.build

  override val fn: (SquashedFetchedRow) => SquashedFetchedRow = _.flattenData(onField, effectiveOrdinalField, isLeft, sampler)
}

case class RemovePlan(
                       override val child: ExecutionPlan,
                       toBeRemoved: Seq[Field]
                     ) extends UnaryPlan(child) with MapPlan {

  override val schema = child.schema -- toBeRemoved

  override val fn: SquashedFetchedRow => SquashedFetchedRow = _.remove(toBeRemoved: _*)
}
