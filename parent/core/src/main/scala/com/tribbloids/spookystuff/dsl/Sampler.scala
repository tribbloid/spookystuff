package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.function.hom.Hom.Mono

import scala.util.Random

trait Sampler extends Mono.Impl[Any, Sampler._I, Sampler._O] {}

object Sampler {

  type _I[T] = Seq[T]
  type _O[T] = Seq[Option[T]]

  object Identity extends Sampler {

    override def apply[T <: Any](arg: In[T]): _O[T] = arg.map(v => Some(v))
  }

  object LeftOuter extends Sampler {

    override def apply[T <: Any](arg: Sampler._I[T]): Out[T] = {

      if (arg.isEmpty) Seq(None)
      else arg.map(v => Some(v))
    }
  }

  def withReplacement[T](seq: Seq[T]): Option[T] = {

    if (seq.isEmpty) return None

    val index = Random.nextInt(seq.size)
    Some(seq(index))
  }

  case class FirstN(n: Int) extends Sampler {

    override def apply[T <: Any](arg: Sampler._I[T]): Out[T] = {

      arg.slice(0, n).map(v => Some(v))
    }

//    def apply(v: Iterable[(Any, Int)]): Iterable[(Any, Int)] =
//      v.slice(0, n)
  }

  // TODO : can be faster
  case class DownsamplingByRatio(ratio: Double) extends Sampler {

    override def apply[T <: Any](arg: Sampler._I[T]): Out[T] = {

      Random
        .shuffle(arg)
        .slice(0, (ratio * arg.size).toInt)
        .map(v => Some(v))
    }
  }

  // TODO : can be faster
  case class DownsamplingToN(n: Int) extends Sampler {

    override def apply[T <: Any](arg: Sampler._I[T]): Out[T] = {
      Random
        .shuffle(arg)
        .slice(0, n)
        .map(v => Some(v))
    }
  }
}
