package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.execution._
import com.tribbloids.spookystuff.extractors._
import org.apache.spark.ml.dsl.utils.refl.SerializingUDT
import org.apache.spark.sql.types.{StructField, StructType}

import scala.language.implicitConversions

object SpookySchema {}

//this is a special StructType that carries more metadata
//TODO: override sqlType, serialize & deserialize to compress into InternalRow
case class SpookySchema(
    @transient ec: SpookyExecutionContext = null,
    fields: List[Field] = List.empty
) extends SerializingUDT[DataRow] {

  import Field._

  def spooky: SpookyContext = ec.spooky

  @transient final lazy val fieldLookup: Map[Symbol, Field] = {

    val withIndices = fields.flatMap(ff => ff.allIndices.map(ii => ii -> ff))

    val grouped = withIndices.groupBy(_._1).view.mapValues(_.head._2).toMap
    grouped
  }

  def getReference(by: Symbol): Option[Ref] = {
    val fieldOpt = fieldLookup.get(by)
    fieldOpt.map { ff =>
      Ref(this, ff)
    }
  }
  def apply(by: Symbol): Ref = {
    getReference(by).getOrElse {
      throw new UnsupportedOperationException(s"field $by does not exist")
    }
  }

  @transient final lazy val indices = fields.flatMap(getReference)

  def filterBy(fn: Ref => Boolean = _.alias.isSelected): SpookySchema = {
    this.copy(
      fields = indices.filter(fn).map(_.field)
    )
  }

  object inSpark {

    lazy val structFields: Seq[StructField] = fields.map(_.toStructField)

    lazy val structType: StructType = {

      StructType(structFields)
    }
  }

  def --(by: Symbol*): SpookySchema = {

    val _fields = by.flatMap(getReference).map(_.field).toSet

    this.copy(
      fields = fields.filter(f => !_fields.contains(f))
    )
  }

  lazy val allRefs: List[Ref] = fields.flatMap(getReference)

  case class ResolveDelta() extends Serializable {
    // TODO: should be "Delta"
    // every new field being resolved should return a new FieldReference
    // not thread safe

    var schema: SpookySchema = SpookySchema.this

    def build: SpookySchema = schema

    def includeField(field: Field*): Seq[Field] = {
      schema = schema.copy(
        fields = schema.fields ++ field
      )
      field
    }

    private def includeOne[R](
        ex: Extractor[R]
    ): Expr[R] = {

      val aliasedEx = ex match {
        case a: HasAlias[_, _] =>
          val alias = a.alias
          ex withAlias alias
        case _ =>
          val names = schema.indices.map(_.alias.name)
          val i = (1 to Int.MaxValue)
            .find(i => !names.contains("_c" + i))
            .get
          ex withAlias Alias("_c" + i)
      }
      val resolved = aliasedEx.resolve(schema)
      val dataType = aliasedEx.resolveType(schema)

      val field = Field(aliasedEx.alias, dataType)
      includeField(field)

      Expr(
        resolved,
        field
      )
    }

    def include[R](exs: Extractor[R]*): Seq[Expr[R]] = {

      exs.map { ex =>
        this.includeOne(ex)
      }
    }
  }
}
