package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._
import org.apache.spark.ml.dsl.utils.refl.ScalaType._
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
  * Created by peng on 7/3/17.
  */
case class Append[+T: ClassTag] private (
    get: Get,
    expr: Extractor[T]
) extends Extractor[Seq[T]] {

  override def resolveType(tt: DataType): DataType = {
    val existingType = expr.resolveType(tt)

    existingType.asArray
  }

  override def resolve(tt: DataType): PartialFunction[FR, Seq[T]] = {
    val getSeqResolved = get.AsSeq.resolve(tt).lift
    val exprResolved = expr.resolve(tt).lift

    { v1: FR =>
      val lastOption = exprResolved.apply(v1)
      val oldOption = getSeqResolved.apply(v1)

      oldOption.toSeq.flatMap { old =>
        SpookyUtils.asIterable[T](old)
      } ++ lastOption
    }
  }

  override def _args: Seq[GenExtractor[_, _]] = Seq(get, expr)
}

object Append {

  def create[T: ClassTag](
      field: Field,
      expr: Extractor[T]
  ): Alias[FR, Seq[T]] = {

    Append[T](Get(field), expr).withAlias(field.!!)
  }
}
