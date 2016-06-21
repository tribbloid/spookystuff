package org.apache.spark.ml.dsl.utils

object FlowUtils {

  def cartesianProductSet[T](xss: Seq[Set[T]]): Set[List[T]] = xss match {
    case Nil => Set(Nil)
    case h :: t => for(
      xh <- h;
      xt <- cartesianProductSet(t)
    )
      yield xh :: xt
  }

  def cartesianProductList[T](xss: Seq[Seq[T]]): Seq[List[T]] = xss match {
    case Nil => List(Nil)
    case h :: t => for(
      xh <- h;
      xt <- cartesianProductList(t)
    )
      yield xh :: xt
  }
}