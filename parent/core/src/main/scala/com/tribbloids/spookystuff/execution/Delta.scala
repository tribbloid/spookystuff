package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.function.Impl
import ai.acyclic.prover.commons.function.Impl.:=>
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.dsl.ForkType
import com.tribbloids.spookystuff.extractors.impl.Get
import com.tribbloids.spookystuff.extractors.{Extractor, Resolved}
import com.tribbloids.spookystuff.row.{DataRow, Field, Sampler, SpookySchema, SquashedRow, TypedField}
import org.apache.spark.ml.dsl.utils.refl.CatalystTypeOps
import org.apache.spark.sql.types.{ArrayType, IntegerType}

trait Delta extends Serializable {

  def fn: SquashedRow :=> SquashedRow
  def outputSchema: SpookySchema

}

object Delta {

  type ToDelta = SpookySchema => Delta

  case class NoDelta(outputSchema: SpookySchema) extends Delta {

    override def fn: SquashedRow :=> SquashedRow = Impl(v => v)
  }

  /**
    * extract parts of each Page and insert into their respective context if a key already exist in old context it will
    * be replaced with the new one.
    *
    * @return
    *   new PageRowRDD
    */
  case class Extract(exs: Seq[Extractor[_]])(val childSchema: SpookySchema) extends Delta {

    val resolver: childSchema.Resolver = childSchema.newResolver
    val _exs: Seq[Resolved[Any]] = resolver.include(exs: _*)

    override val outputSchema: SpookySchema = {
      resolver.build
    }

    override lazy val fn: SquashedRow :=> SquashedRow = Impl { v =>
      {
        v.withCtx(outputSchema.spooky): v._WithCtx
      }.extract(_exs: _*)
    }
  }
  case class ExplodeData(
      onField: Field,
      ordinalField: Field,
      sampler: Sampler[Any],
      forkType: ForkType
  )(val childSchema: SpookySchema)
      extends Delta {

    val resolver: childSchema.Resolver = childSchema.newResolver

    val _on: TypedField = {
      val flattenType = CatalystTypeOps(Get(onField).resolveType(childSchema)).unboxArrayOrMap
      val tf = TypedField(onField.!!, flattenType)

      resolver.includeTyped(tf).head
    }

    val effectiveOrdinalField: Field = Option(ordinalField) match {
      case Some(ff) =>
        ff.copy(isOrdinal = true)
      case None =>
        Field(_on.self.name + "_ordinal", isTransient = true, isOrdinal = true)
    }

    val _: TypedField = resolver.includeTyped(TypedField(effectiveOrdinalField, ArrayType(IntegerType))).head

    override val outputSchema: SpookySchema = resolver.build

    override lazy val fn: SquashedRow :=> SquashedRow = Impl { v =>
      val result = v.explodeData(onField, effectiveOrdinalField, forkType, sampler)
      result
    }
  }

  case class ExplodeScope(
      scopeFn: DataRow.WithScope => Seq[DataRow.WithScope]
  )(val childSchema: SpookySchema)
      extends Delta {

    override def outputSchema: SpookySchema = childSchema

    override lazy val fn: SquashedRow :=> SquashedRow = Impl { v =>
      v.explodeScope(scopeFn)
    }
  }

  case class Remove(
      toBeRemoved: Seq[Field]
  )(val childSchema: SpookySchema)
      extends Delta {

    override val outputSchema: SpookySchema = childSchema -- toBeRemoved

    override lazy val fn: SquashedRow :=> SquashedRow = Impl { v =>
      v.remove(toBeRemoved: _*)
    }
  }

  case class SaveContent(
      path: Extractor[String],
      extension: Extractor[String],
      doc: Extractor[Doc],
      overwrite: Boolean
  )(val childSchema: SpookySchema)
      extends Delta {

    val resolver: childSchema.Resolver = childSchema.newResolver

    val _ext: Resolved[String] = resolver.include(extension).head
    val _path: Resolved[String] = resolver.include(path).head
    val _docExpr: Resolved[Doc] = resolver.include(doc).head

    override val outputSchema: SpookySchema = childSchema

    override lazy val fn: SquashedRow :=> SquashedRow = Impl { v =>
      val withCtx = v.withCtx(childSchema.spooky)

      withCtx.unSquash
        .foreach { fetchedRow =>
          var pathStr: Option[String] = _path.lift(fetchedRow).map(_.toString).map { str =>
            val splitted = str.split(":")
            if (splitted.size <= 2) str
            else splitted.head + ":" + splitted.slice(1, Int.MaxValue).mkString("%3A")
          }

          val ext = _ext.lift(fetchedRow).getOrElse("")
          if (ext.nonEmpty) pathStr = pathStr.map(_ + "." + ext)

          pathStr.foreach { str =>
            val docOpt = _docExpr.lift(fetchedRow)

            docOpt.foreach(_.save(outputSchema.spooky, overwrite)(Seq(str)))
          }
        }
      v
    }
  }
}
