package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors.GenExtractor.Leaf
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._
import org.apache.spark.ml.dsl.utils.refl.CatalystTypeOps
import org.apache.spark.sql.types._

/**
  * Created by peng on 7/3/17.
  */
case class Get(src: Field) extends Leaf[FR, Any] {

  override def resolveType(tt: DataType): DataType = tt match {
    case schema: SpookySchema =>
      schema
        .typedFor(src)
        .orElse {
          schema.typedFor(src.*)
        }
        .map(_.dataType)
        .getOrElse(NullType)
    case _ =>
      throw new UnsupportedOperationException("Can only resolve type against SchemaContext")
  }

  override def resolve(tt: DataType): PartialFunction[FR, Any] = Unlift(v => v.dataRow.orWeak(src))

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
