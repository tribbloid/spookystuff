package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.execution.MapPlan.RowMapperFactory
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.extractors.{Expr, Extractor}
import com.tribbloids.spookystuff.row._
import org.apache.spark.ml.dsl.utils.refl.CatalystTypeOps

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

object MapPlan extends CatalystTypeOps.ImplicitMixin {

  type RowMapperBase = SquashedRow => SquashedRow

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

    val resolver: childSchema.ResolveDelta = childSchema.ResolveDelta
    val _exs: Seq[Expr[Any]] = resolver.include(exs: _*)

    override val schema: SpookySchema = {
      resolver.build
    }

    override def apply(v: SquashedRow): SquashedRow = {
      v.WSchema(schema).extract(_exs: _*)
    }
  }

  case class Flatten(
      on: Alias,
      ordinal: Alias,
      sampler: Sampler[Any],
      isLeft: Boolean
  )(val childSchema: SpookySchema)
      extends RowMapper {

    val resolver: childSchema.ResolveDelta = childSchema.ResolveDelta

    val _on: Field = {
      val flattenType = Get(on).resolveType(childSchema).unboxArrayOrMap
      val tf = Field(on, flattenType)

      resolver.includeField(tf)
      tf
    }

    val effectiveOrdinal: Alias = Option(ordinal) match {
      case Some(ff) =>
        ff.copy(isOrdinal = true)
      case None =>
        Alias(_on.alias.name + "_ordinal", isWeak = true, isOrdinal = true)
    }

    override val schema: SpookySchema = resolver.build

    override def apply(v: SquashedRow): SquashedRow =
      v.explode(on, effectiveOrdinal, isLeft, sampler)
  }

  object Flatten {}

  case class Remove(
      toBeRemoved: Seq[Field.Symbol]
  )(val childSchema: SpookySchema)
      extends RowMapper {

    override val schema: SpookySchema = childSchema -- toBeRemoved

    override def apply(v: SquashedRow): SquashedRow = v.remove(toBeRemoved: _*)
  }

  case class SavePages(
      path: Extractor[String],
      extension: Extractor[String],
      page: Extractor[Doc],
      overwrite: Boolean
  )(val childSchema: SpookySchema)
      extends RowMapper {

    val resolver: childSchema.ResolveDelta = childSchema.ResolveDelta

    val _ext: Expr[String] = resolver.include(extension).head
    val _path: Expr[String] = resolver.include(path).head
    val _pageExpr: Expr[Doc] = resolver.include(page).head

    override val schema: SpookySchema = childSchema

    override def apply(v: SquashedRow): SquashedRow = {
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
