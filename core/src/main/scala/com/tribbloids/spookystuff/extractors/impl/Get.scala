package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors.GenExtractor.Leaf
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row.{SpookySchema, _}
import com.tribbloids.spookystuff.utils.refl.ScalaType._
import org.apache.spark.sql.types._

import scala.collection.TraversableOnce

/**
  * Created by peng on 7/3/17.
  */
case class Get(field: Field) extends Leaf[FR, Any] {

  override def resolveType(tt: DataType): DataType = tt match {
    case schema: SpookySchema =>
      schema
        .typedFor(field)
        .orElse{
          schema.typedFor(field.*)
        }
        .map(_.dataType)
        .getOrElse(NullType)
    case _ =>
      throw new UnsupportedOperationException("Can only resolve type against SchemaContext")
  }

  override def resolve(tt: DataType): PartialFunction[FR, Any] = Unlift(
    v =>
      v.dataRow.orWeak(field)
  )

  def GetSeq: GenExtractor[FR, Seq[Any]] = this.andOptionTyped[Any, Seq[Any]](
    {
      case v: TraversableOnce[Any] => Some(v.toSeq)
      case v: Array[Any] => Some(v.toSeq)
      case _ => None
    },
    {
      _.ensureArray
    }
  )

  def AsSeq: GenExtractor[FR, Seq[Any]] = this.andOptionTyped[Any, Seq[Any]](
    {
      case v: TraversableOnce[Any] => Some(v.toSeq)
      case v: Array[Any] => Some(v.toSeq)
      case v@ _ => Some(Seq(v))
    },
    {
      _.asArray
    }
  )
}
