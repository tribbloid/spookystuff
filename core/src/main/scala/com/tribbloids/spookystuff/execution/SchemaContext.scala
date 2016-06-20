package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

case class SchemaContext(
                          spooky: SpookyContext,
                          map: ListMap[Field, DataType] = ListMap.empty
                        )
  extends DataType {

  override def defaultSize: Int = 0

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

  override def asNullable = this

  def filterFields(filter: Field => Boolean = _.isSelected): SchemaContext = {
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
            tuple._2
          )
      }

    StructType(structFields)
  }

  def -- (field: Iterable[Field]): SchemaContext = this.copy(
    map = map -- field
  )

  def newResolver: Resolver = new Resolver()

  class Resolver extends Serializable {

    val buffer: LinkedMap[Field, DataType] = LinkedMap()
    buffer ++= SchemaContext.this.map.toSeq
    //    val lookup: mutable.HashMap[Extractor[_], Resolved[_]] = mutable.HashMap()

    def build: SchemaContext = SchemaContext.this.copy(map = ListMap(buffer.toSeq: _*))

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

    def resolveTyped(typed: TypedField*) = {
      typed.map{
        t =>
          val resolvedField = resolveField(t.self)
          verifyFieldConsistency(resolvedField, t.dataType)
          val result = TypedField(resolvedField, t.dataType)
          buffer += result.self -> result.dataType
          result
      }
    }

    private def _resolve[R](
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
      val resolved = alias.resolve(SchemaContext.this)
      val dataType = alias.applyType(SchemaContext.this)

      verifyFieldConsistency(alias.field, dataType)

      buffer += alias.field -> dataType
      Resolved(
        resolved,
        TypedField(alias.field, dataType)
      )
    }

    def resolve[R](exs: Extractor[R]*): Seq[Resolved[R]] = {

      exs.map {
        ex =>
          this._resolve(ex)
      }
    }
  }

  def verifyFieldConsistency(resolvedField: Field, t: DataType): Unit = {
    if (resolvedField.conflictResolving == Field.Overwrite) {
      SchemaContext.this.map.get(resolvedField).foreach {
        existingType =>
          require(
            t == existingType,
            s"partially overwriting field ${resolvedField.name} with different data type (old: $existingType, new: $t), set conflictResolving=Replace to fix it"
          )
      }
    }
  }

  //use it after Row-based data representation
  object Lookup {
    implicit def fieldToTyped(field: Field): TypedField = SchemaContext.this.typedFor(field).get
    implicit def fieldToIndexed(field: Field): IndexedField = SchemaContext.this.indexedFor(field).get
  }
}



