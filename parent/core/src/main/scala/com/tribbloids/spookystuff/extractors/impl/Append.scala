package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.utils.SpookyUtils

import scala.reflect.ClassTag

/**
  * Created by peng on 7/3/17.
  */
case class Append[T: ClassTag](
    get: Get,
    getNew: Extractor[T]
) extends Fold[Seq[Any], T, Seq[T]] {

  override val getOld: Extractor[Seq[Any]] = get.AsSeq

  override def foldFn(oldV: => Option[Seq[Any]], newV: => Option[T]): Option[Seq[T]] = {

    val result = oldV.toSeq.flatMap { old =>
      SpookyUtils.asIterable[T](old)
    } ++ newV

    Some(result)
  }
}

object Append {}
