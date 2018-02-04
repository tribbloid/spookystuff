package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.extractors.{Extractor, Resolved}
import com.tribbloids.spookystuff.row._
import org.apache.spark.sql.types.{ArrayType, IntegerType}

case class MapPlan(
                    override val child: ExecutionPlan,
                    mapFactory: RowMapperFactory
                  ) extends UnaryPlan(child) {


  @transient lazy val mapFn = {
    mapFactory.apply(child.schema)
  }

  override val schema: SpookySchema = mapFn.schema

  final override def doExecute(): SquashedFetchedRDD = {

    val rdd = child.rdd()
    rdd.map {
      mapFn
    }
  }
}

object MapPlan {

  trait RowMapper extends (SquashedFetchedRow => SquashedFetchedRow) with Serializable {
    def schema: SpookySchema
  }

  /**
    * extract parts of each Page and insert into their respective context
    * if a key already exist in old context it will be replaced with the new one.
    *
    * @return new PageRowRDD
    */
  case class Extract(exs: Seq[Extractor[_]])(val childSchema: SpookySchema) extends RowMapper {

    val resolver = childSchema.newResolver
    val _exs: Seq[Resolved[Any]] = resolver.include[Any](exs: _*)

    override val schema: SpookySchema = {
      resolver.build
    }

    override def apply(v: SquashedFetchedRow) = {
      v.WSchema(schema).extract(_exs: _*)
    }
  }

  case class Flatten(
                      onField: Field,
                      ordinalField: Field,
                      sampler: Sampler[Any],
                      isLeft: Boolean
                    )(val childSchema: SpookySchema) extends RowMapper {


    import org.apache.spark.ml.dsl.utils.refl.ScalaType._

    val resolver = childSchema.newResolver

    val _on: TypedField = {
      val flattenType = Get(onField).resolveType(childSchema).unboxArrayOrMap
      val tf = TypedField(onField.!!, flattenType)

      resolver.includeTyped(tf).head
    }

    val effectiveOrdinalField = Option(ordinalField) match {
      case Some(ff) =>
        ff.copy(isOrdinal = true)
      case None =>
        Field(_on.self.name + "_ordinal", isWeak = true, isOrdinal = true)
    }

    val _ordinal: TypedField = resolver.includeTyped(TypedField(effectiveOrdinalField, ArrayType(IntegerType))).head

    override val schema = resolver.build

    override def apply(v: SquashedFetchedRow) =
      v.flattenData(onField, effectiveOrdinalField, isLeft, sampler)
  }

  case class Remove(
                     toBeRemoved: Seq[Field]
                   )(val childSchema: SpookySchema) extends RowMapper {

    override val schema = childSchema -- toBeRemoved

    override def apply(v: SquashedFetchedRow) = v.remove(toBeRemoved: _*)
  }
}
