package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._
import com.tribbloids.spookystuff.utils.SpookyUtils

import scala.reflect.ClassTag

//trait Merge[+T: ClassTag]  extends Extractor[T] {
//
// val get: Get
//
//  val ex: Extractor[T]
//
//  val mergeFn: (Any, T) => T
//
//  override def _args: Seq[GenExtractor[_, _]] = Seq(get, ex)
//
//  override def resolveType(tt: DataType): DataType = {
//    val existingType = ex.resolveType(tt)
//
//    existingType
//  }
//
//  override def resolve(tt: DataType): PartialFunction[FR, Seq[T]] = {
//    val getSeqResolved = get.AsSeq.resolve(tt).lift
//    val exprResolved = ex.resolve(tt).lift
//
//    {
//      case v1: FR =>
//        val lastOption = exprResolved.apply(v1)
//        val oldOption = getSeqResolved.apply(v1)
//
//        oldOption.toSeq.flatMap { old =>
//          SpookyUtils.asIterable[T](old)
//        } ++ lastOption
//    }
//  }
//
//}

/**
  * Created by peng on 7/3/17.
  */
case class Append[T: ClassTag] private (
    get: Get,
    ex: Extractor[T]
) extends Extractor[Seq[T]] {

  override def resolveType(tt: DataType): DataType = {
    val existingType = ex.resolveType(tt)

    existingType.asArray
  }

  @transient lazy val getSeq: GenExtractor[FR, Seq[Any]] = get.AsSeq

  override def resolve(tt: DataType): PartialFunction[FR, Seq[T]] = {
    val getSeqResolved: FR => Option[Seq[Any]] = getSeq.resolve(tt).lift
    val exprResolved: FR => Option[T] = ex.resolve(tt).lift

    {
      case v1: FR =>
        val lastOption = exprResolved.apply(v1)
        val oldOption = getSeqResolved.apply(v1)

        oldOption.toSeq.flatMap { old =>
          SpookyUtils.asIterable[T](old)
        } ++ lastOption
    }
  }

  override def _args: Seq[GenExtractor[_, _]] = Seq(get, ex)
}

object Append {}
