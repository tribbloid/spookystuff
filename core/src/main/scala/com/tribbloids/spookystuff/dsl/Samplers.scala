package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.row.Sampler

import scala.util.Random

/**
  * Created by peng on 06/04/16.
  */
// TODO: Sampler should be a magnet class
object Samplers {

  def withReplacement[T](seq: Seq[T]): Option[T] = {

    if (seq.isEmpty) return None

    val index = Random.nextInt(seq.size)
    Some(seq(index))
  }

  case class FirstN(n: Int) extends Sampler[Any] {

    def apply(v: Iterable[(Any, Int)]): Iterable[(Any, Int)] =
      v.slice(0, n)
  }

  // TODO : can be faster
  case class DownsamplingByRatio(ratio: Double) extends Sampler[Any] {
    def apply(v: Iterable[(Any, Int)]): Iterable[(Any, Int)] =
      Random
        .shuffle(v)
        .slice(0, (ratio * v.size).toInt)
  }

  // TODO : can be faster
  case class DownsamplingToN(n: Int) extends Sampler[Any] {
    def apply(v: Iterable[(Any, Int)]): Iterable[(Any, Int)] =
      Random
        .shuffle(v)
        .slice(0, n)
  }
}
