package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.extractors.GenExtractor.Leaf
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._
import org.apache.spark.sql.types._

/**
  * Created by peng on 7/3/17.
  */
case class Get(field: Field) extends Leaf[FR, Any] {

  override def resolveType(tt: DataType): DataType = tt match {
    case schema: SpookySchema =>
      schema
        .typedFor(field)
        .orElse {
          schema.typedFor(field.*)
        }
        .map(_.dataType)
        .getOrElse(NullType)
    case _ =>
      throw new UnsupportedOperationException("Can only resolve type against SchemaContext")
  }

  override def resolve(tt: DataType): PartialFunction[FR, Any] = Unlift(v => v.dataRow.orWeak(field))

  def GetSeq: GenExtractor[FR, Seq[Any]] = this.andOptionTyped[Any, Seq[Any]](
    {
      case v: IterableOnce[Any] => Some(v.toSeq)
      case v: Array[Any]        => Some(v.toSeq)
      case _                    => None
    },
    v => CatalystTypeOps(v).ensureArray
  )

  def AsSeq: GenExtractor[FR, Seq[Any]] = this.andOptionTyped[Any, Seq[Any]](
    {
      case v: IterableOnce[Any] => Some(v.toSeq)
      case v: Array[Any]        => Some(v.toSeq)
      case v @ _                => Some(Seq(v))
    },
    v => CatalystTypeOps(v).asArray
  )
}
