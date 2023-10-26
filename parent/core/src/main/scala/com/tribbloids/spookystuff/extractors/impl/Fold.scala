package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.utils.SpookyUtils

import scala.reflect.ClassTag

abstract class Fold[T1, T2, R: ClassTag] extends Extractor[R] {

  val getOld: Extractor[T1]

  val getNew: Extractor[T2]

  def foldFn(
      oldV: => Option[T1],
      newV: => Option[T2]
  ): Option[R]

  override def _args: Seq[GenExtractor[_, _]] = Seq(getOld, getNew)

  override def resolveType(tt: DataType): DataType = {
    val existingType = getNew.resolveType(tt)

    existingType
  }

  override def resolve(tt: DataType): PartialFunction[FR, R] = {
    lazy val oldResolved = getOld.resolve(tt).lift
    lazy val newResolved = getNew.resolve(tt).lift

    Unlift { v1: FR =>
      lazy val oldOpt = oldResolved.apply(v1)
      lazy val newOpt = newResolved.apply(v1)

      foldFn(oldOpt, newOpt)
    }
  }

}
