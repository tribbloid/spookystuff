package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.execution.MapPlan.RowMapperFactory
import com.tribbloids.spookystuff.extractors.{Extractor, Resolved}
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.row._
import org.apache.spark.sql.types.{ArrayType, IntegerType}

case class MapPlan(
    override val child: ExecutionPlan,
    rowMapperFactory: RowMapperFactory
) extends UnaryPlan(child) {

  @transient lazy val mapFn: MapPlan.RowMapper = {
    rowMapperFactory.apply(child.schema)
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

  type RowMapperBase = SquashedFetchedRow => SquashedFetchedRow

  trait RowMapper extends RowMapperBase with Serializable {
    def schema: SpookySchema
  }

  type RowMapperFactory = SpookySchema => RowMapper

  def optimised(
      child: ExecutionPlan,
      rowMapperFactory: RowMapperFactory
  ): UnaryPlan = {

    child match {
      case plan: ExplorePlan if !plan.isCached =>
        plan.copy(rowMapperFactories = plan.rowMapperFactories :+ rowMapperFactory)
      case _ =>
        MapPlan(child, rowMapperFactory)
    }
  }

  /**
    * extract parts of each Page and insert into their respective context if a key already exist in old context it will
    * be replaced with the new one.
    *
    * @return
    *   new PageRowRDD
    */
  case class Extract(exs: Seq[Extractor[_]])(val childSchema: SpookySchema) extends RowMapper {

    val resolver: childSchema.Resolver = childSchema.newResolver
    val _exs: Seq[Resolved[Any]] = resolver.include(exs: _*)

    override val schema: SpookySchema = {
      resolver.build
    }

    override def apply(v: SquashedFetchedRow): SquashedFetchedRow = {
      v.WSchema(schema).extract(_exs: _*)
    }
  }

  case class Flatten(
      onField: Field,
      ordinalField: Field,
      sampler: Sampler[Any],
      isLeft: Boolean
  )(val childSchema: SpookySchema)
      extends RowMapper {

    import org.apache.spark.ml.dsl.utils.refl.ScalaType._

    val resolver: childSchema.Resolver = childSchema.newResolver

    val _on: TypedField = {
      val flattenType = Get(onField).resolveType(childSchema).unboxArrayOrMap
      val tf = TypedField(onField.!!, flattenType)

      resolver.includeTyped(tf).head
    }

    val effectiveOrdinalField: Field = Option(ordinalField) match {
      case Some(ff) =>
        ff.copy(isOrdinal = true)
      case None =>
        Field(_on.self.name + "_ordinal", isWeak = true, isOrdinal = true)
    }

    val _ordinal: TypedField = resolver.includeTyped(TypedField(effectiveOrdinalField, ArrayType(IntegerType))).head

    override val schema: SpookySchema = resolver.build

    override def apply(v: SquashedFetchedRow): SquashedFetchedRow =
      v.flattenData(onField, effectiveOrdinalField, isLeft, sampler)
  }

  case class Remove(
      toBeRemoved: Seq[Field]
  )(val childSchema: SpookySchema)
      extends RowMapper {

    override val schema: SpookySchema = childSchema -- toBeRemoved

    override def apply(v: SquashedFetchedRow): SquashedFetchedRow = v.remove(toBeRemoved: _*)
  }

  case class SavePages(
      path: Extractor[String],
      extension: Extractor[String],
      page: Extractor[Doc],
      overwrite: Boolean
  )(val childSchema: SpookySchema)
      extends RowMapper {

    val resolver: childSchema.Resolver = childSchema.newResolver

    val _ext: Resolved[String] = resolver.include(extension).head
    val _path: Resolved[String] = resolver.include(path).head
    val _pageExpr: Resolved[Doc] = resolver.include(page).head

    override val schema: SpookySchema = childSchema

    override def apply(v: SquashedFetchedRow): SquashedFetchedRow = {
      val wSchema = v.WSchema(schema)

      wSchema.unsquash
        .foreach { pageRow =>
          var pathStr: Option[String] = _path.lift(pageRow).map(_.toString).map { str =>
            val splitted = str.split(":")
            if (splitted.size <= 2) str
            else splitted.head + ":" + splitted.slice(1, Int.MaxValue).mkString("%3A")
          }

          val ext = _ext.lift(pageRow).getOrElse("")
          if (ext.nonEmpty) pathStr = pathStr.map(_ + "." + ext)

          pathStr.foreach { str =>
            val page = _pageExpr.lift(pageRow)

            schema.spooky.spookyMetrics.pagesSaved += 1

            page.foreach(_.save(Seq(str), overwrite)(schema.spooky))
          }
        }
      v
    }
  }
}
