package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors._

abstract class Fold[T1, T2, R]() extends Extractor[R] {

  val getOld: Extractor[T1]

  val getNew: Extractor[T2]

  def foldType(
      oldT: => DataType,
      newT: => DataType
  ): DataType

  def fold(
      oldV: => Option[T1],
      newV: => Option[T2]
  ): Option[R]

  override def _args: Seq[GenExtractor[_, _]] = Seq(getOld, getNew)

  override def resolveType(tt: DataType): DataType = {
    def oldT = getOld.resolveType(tt)
    def newT = getNew.resolveType(tt)

    foldType(oldT, newT)
  }

  override def resolve(tt: DataType): PartialFunction[FR, R] = {
    lazy val oldResolved = getOld.resolve(tt).lift
    lazy val newResolved = getNew.resolve(tt).lift

    Unlift { v1: FR =>
      lazy val oldOpt = oldResolved.apply(v1)
      lazy val newOpt = newResolved.apply(v1)

      fold(oldOpt, newOpt)
    }
  }
}
