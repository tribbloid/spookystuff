package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.ScalaType._
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
  * Created by peng on 7/3/17.
  */
case class AppendExpr[+T: ClassTag] private(
                                             get: GetExpr,
                                             expr: Extractor[T]
                                           ) extends Extractor[Seq[T]] {

                                             override def resolveType(tt: DataType): DataType = {
                                               val existingType = expr.resolveType(tt)

                                               existingType.asArray
                                             }

                                             override def resolve(tt: DataType): PartialFunction[FR, Seq[T]] = {
                                               val getSeqResolved = get.AsSeqExpr.resolve(tt).lift
                                               val exprResolved = expr.resolve(tt).lift

                                               PartialFunction({
                                                 v1: FR =>
                                                   val lastOption = exprResolved.apply(v1)
                                                   val oldOption = getSeqResolved.apply(v1)

                                                   oldOption.toSeq.flatMap{
                                                     old =>
                                                       SpookyUtils.asIterable[T](old)
                                                   } ++ lastOption
                                               })
                                             }

                                             override def _args: Seq[GenExtractor[_, _]] = Seq(get, expr)
                                           }

object AppendExpr {

  def create[T: ClassTag](
                           field: Field,
                           expr: Extractor[T]
                         ): Alias[FR, Seq[T]] = {

    AppendExpr[T](GetExpr(field), expr).withAlias(field.!!)
  }
}