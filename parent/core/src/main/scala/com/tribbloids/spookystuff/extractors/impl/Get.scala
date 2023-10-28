package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors.GenExtractor.Leaf
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._
import org.apache.spark.ml.dsl.utils.refl.CatalystTypeOps

/**
  * Created by peng on 7/3/17.
  */
case class Get(
    symbol: Field.Symbol
) extends Leaf[FR, Any] {
  // TODO: merge into Field.Symbol

  override def resolveType(tt: DataType): DataType = tt match {
    case schema: SpookySchema =>
      schema
        .getReference(symbol)
        .orElse {
          schema.getReference(symbol.*)
        }
        .map(_.dataType)
        .getOrElse(
          throw new UnsupportedOperationException(s"field $symbol does not exist")
        )
    case _ =>
      throw new UnsupportedOperationException("Can only resolve type against SpookySchema")
  }

  override def resolve(tt: DataType): PartialFunction[FR, Any] = Unlift(v => v.dataRow.orWeak(symbol))

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
