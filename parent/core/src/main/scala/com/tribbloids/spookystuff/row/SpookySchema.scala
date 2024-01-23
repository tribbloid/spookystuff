package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.extractors._
import org.apache.spark.ml.dsl.utils.refl.ScalaUDT
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

//this is a special StructType that carries more metadata
//TODO: override sqlType, serialize & deserialize to compress into InternalRow
case class SpookySchema(
    ec: SpookyExecutionContext,
    fieldTypes: ListMap[Field, DataType] = ListMap.empty
) extends ScalaUDT[DataRow] {

  def spooky: SpookyContext = ec.spooky

  final def fields: List[Field] = fieldTypes.keys.toList
  final def typedFields: List[TypedField] = fieldTypes.iterator.toList.map(tuple => TypedField(tuple._1, tuple._2))
  final def indexedFields: List[IndexedField] = typedFields.zipWithIndex

  final def typedFor(field: Field): Option[TypedField] = {
    fieldTypes.get(field).map {
      TypedField(field, _)
    }
  }
  final def indexedFor(field: Field): Option[IndexedField] = {
    indexedFields.find(_._1.self == field)
  }

  def filterFields(filter: Field => Boolean): SpookySchema = {
    this.copy(
      fieldTypes = ListMap(fieldTypes.view.filterKeys(filter).toSeq: _*)
    )
  }

//  lazy val evictTransientFields: SpookySchema = filterFields(_.isNonTransient)

  lazy val structFields: Seq[StructField] = fieldTypes.toSeq
    .map { tuple =>
      StructField(
        tuple._1.name,
        tuple._2.reified
      )
    }

  lazy val structType: StructType = {

    StructType(structFields)
  }

  def --(field: Iterable[Field]): SpookySchema = this.copy(
    fieldTypes = fieldTypes -- field
  )

  def newResolver: Resolver = new Resolver()

  class Resolver extends Serializable {

    val buffer: LinkedMap[Field, DataType] = LinkedMap()
    buffer ++= SpookySchema.this.fieldTypes.toSeq
    //    val lookup: mutable.HashMap[Extractor[_], Resolved[_]] = mutable.HashMap()

    def build: SpookySchema = SpookySchema.this.copy(fieldTypes = ListMap(buffer.toSeq: _*))

    private def includeField(field: Field): Field = {

      val existingOpt = buffer.keys.find(_ == field)
      val crOpt = existingOpt.map { existing =>
        field.effectiveConflictResolving(existing)
      }

      val revised = crOpt
        .map(cr => field.copy(conflictResolving = cr))
        .getOrElse(field)

      revised
    }

    def includeTyped(typed: TypedField*): Seq[TypedField] = {
      typed.map { t =>
        val resolvedField = includeField(t.self)
        mergeType(resolvedField, t.dataType)
        val result = TypedField(resolvedField, t.dataType)
        buffer += result.self -> result.dataType
        result
      }
    }

    private def _include[R](
        ex: Extractor[R]
    ): Resolved[R] = {

      val alias = ex match {
        case a: Alias[_, _] =>
          val resolvedField = includeField(a.field)
          ex withAlias resolvedField
        case _ =>
          val names = buffer.keys.toSeq.map(_.name)
          val i = (1 to Int.MaxValue)
            .find(i => !names.contains("_c" + i))
            .get
          ex withAlias Field("_c" + i)
      }
      val resolved = alias.resolve(SpookySchema.this)
      val dataType = alias.resolveType(SpookySchema.this)

      val mergedType = mergeType(alias.field, dataType)

      buffer += alias.field -> mergedType
      Resolved(
        resolved,
        TypedField(alias.field, mergedType)
      )
    }

    def include[R](exs: Extractor[R]*): Seq[Resolved[R]] = {

      exs.map { ex =>
        this._include(ex)
      }
    }
  }

  def mergeType(resolvedField: Field, dataType: DataType): DataType = {

    val existingTypeOpt = SpookySchema.this.fieldTypes.get(resolvedField)

    (existingTypeOpt, resolvedField.conflictResolving) match {
      case (Some(existingType), Field.ReplaceIfNotNull) =>
        if (dataType == existingType) dataType
        else if (dataType.reified == existingType.reified) dataType.reified
        else
          throw new IllegalArgumentException(
            s"""
             |Overwriting field ${resolvedField.name} with inconsistent type:
             |old: $existingType
             |new: $dataType
             |set conflictResolving=Replace to fix it
             """.stripMargin
          )
      case _ =>
        dataType
    }
  }

  // use it after Row-based data representation
  object ImplicitLookup {
    implicit def fieldToTyped(field: Field): TypedField = SpookySchema.this.typedFor(field).get
    implicit def fieldToIndexed(field: Field): IndexedField = SpookySchema.this.indexedFor(field).get
  }
}
