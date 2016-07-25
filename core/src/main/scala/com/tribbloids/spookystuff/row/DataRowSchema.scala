package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.utils.ScalaUDT
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

//this is a special StructType that carries more metadata
//TODO: override sqlType, serialize & deserialize to compress into InternalRow
case class DataRowSchema(
                          spooky: SpookyContext,
                          map: ListMap[Field, DataType] = ListMap.empty
                        ) extends ScalaUDT[DataRow] {

  import com.tribbloids.spookystuff.utils.ImplicitUtils._

  final def fields: List[Field] = map.keys.toList
  final def typedFields: List[TypedField] = map.iterator.toList.map(tuple => TypedField(tuple._1, tuple._2))
  final def indexedFields: List[IndexedField] = typedFields.zipWithIndex

  final def typedFor(field: Field): Option[TypedField] = {
    map.get(field).map {

      TypedField(field, _)
    }
  }
  final def indexedFor(field: Field): Option[IndexedField] = {
    indexedFields.find(_._1.self == field)
  } //TODO: not efficient

  def filterFields(filter: Field => Boolean = _.isSelected): DataRowSchema = {
    this.copy(
      map = ListMap(map.filterKeys(filter).toSeq: _*)
    )
  }

  def toStructType: StructType = {
    val structFields = map
      .toSeq
      .map {
        tuple =>
          StructField(
            tuple._1.name,
            tuple._2.reify
          )
      }

    StructType(structFields)
  }

  def -- (field: Iterable[Field]): DataRowSchema = this.copy(
    map = map -- field
  )

  def newResolver: Resolver = new Resolver()

  class Resolver extends Serializable {

    val buffer: LinkedMap[Field, DataType] = LinkedMap()
    buffer ++= DataRowSchema.this.map.toSeq
    //    val lookup: mutable.HashMap[Extractor[_], Resolved[_]] = mutable.HashMap()

    def build: DataRowSchema = DataRowSchema.this.copy(map = ListMap(buffer.toSeq: _*))

    private def resolveField(field: Field): Field = {

      val existingOpt = buffer.keys.find(_ == field)
      val crOpt = existingOpt.map {
        existing =>
          field.effectiveConflictResolving(existing)
      }

      val revised = crOpt.map(
        cr =>
          field.copy(conflictResolving = cr)
      )
        .getOrElse(field)

      revised
    }

    def includeTyped(typed: TypedField*) = {
      typed.map{
        t =>
          val resolvedField = resolveField(t.self)
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
        case a: Alias[_,_] =>
          val resolvedField = resolveField(a.field)
          ex withAlias resolvedField
        case _ =>
          val names = buffer.keys.toSeq.map(_.name)
          val i = (1 to Int.MaxValue).find(
            i =>
              !names.contains("_c" + i)
          ).get
          ex withAlias Field("_c" + i)
      }
      val resolved = alias.resolve(DataRowSchema.this)
      val dataType = alias.resolveType(DataRowSchema.this)

      val mergedType = mergeType(alias.field, dataType)

      buffer += alias.field -> mergedType
      Resolved(
        resolved,
        TypedField(alias.field, mergedType)
      )
    }

    def include[R](exs: Extractor[R]*): Seq[Resolved[R]] = {

      exs.map {
        ex =>
          this._include(ex)
      }
    }
  }

  def mergeType(resolvedField: Field, dataType: DataType): DataType = {

    val existingTypeOpt = DataRowSchema.this.map.get(resolvedField)

    (existingTypeOpt, resolvedField.conflictResolving) match {
      case (Some(existingType), Field.Overwrite) =>
        if (dataType == existingType) dataType
        else if (dataType.reify == existingType.reify) dataType.reify
        else throw new IllegalArgumentException(
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

  //use it after Row-based data representation
  object ImplicitLookup {
    implicit def fieldToTyped(field: Field): TypedField = DataRowSchema.this.typedFor(field).get
    implicit def fieldToIndexed(field: Field): IndexedField = DataRowSchema.this.indexedFor(field).get
  }
}