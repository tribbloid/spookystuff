package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.utils.EqualBy
import org.apache.spark.sql.types.{DataType, Metadata, StructField}

import java.util.UUID
import scala.language.implicitConversions

case class Field(
    alias: Alias,
    dataType: DataType,
    metaData: Metadata = Metadata.empty
) extends Field.Symbol
    with EqualBy {

  val id: UUID = UUID.randomUUID()

  override protected def _equalBy: UUID = id

  lazy val toStructField: StructField = StructField(
    alias.name,
    dataType
  )

  lazy val allIndices: Seq[Field.Symbol] = Seq(this) ++ Seq(alias)

}

object Field {

  trait Symbol {
    // TODO: merge with `Get` as a expression

    def alias: Alias
  }

  object Symbol {

    implicit def unbox(v: Symbol): Alias = v.alias
  }

  case class Ref(
      schema: SpookySchema,
      field: Field
  ) {

    import schema._

    val leftI: Int = fields.indexOf(field)

    val rightI: Int = fields.size - leftI - 1
  }

  object Ref {

    implicit def unbox(v: Ref): Field = v.field
  }
}
