package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.function.hom.Hom

import scala.util.Random

trait Sampler extends Hom.Impl.Poly1[Sampler.In, Sampler.Out] {} // TODO: should be DownSampling

object Sampler {

  type In[T] = Seq[T]
  type Out[T] = Seq[T]

  object Identity extends Sampler {

    override def apply[T <: Any](arg: In[T]): Out[T] = arg
  }

  def withReplacement[T](seq: Seq[T]): Option[T] = {

    if (seq.isEmpty) return None

    val index = Random.nextInt(seq.size)
    Some(seq(index))
  }

  case class FirstN(n: Int) extends Sampler {

    override def apply[T <: Any](arg: Sampler.In[T]): Out[T] = {

      arg.slice(0, n)
    }

//    def apply(v: Iterable[(Any, Int)]): Iterable[(Any, Int)] =
//      v.slice(0, n)
  }

  // TODO : can be faster
  case class DownsamplingByRatio(ratio: Double) extends Sampler {

    override def apply[T <: Any](arg: Sampler.In[T]): Out[T] = {

      Random
        .shuffle(arg)
        .slice(0, (ratio * arg.size).toInt)
    }
  }

  // TODO : can be faster
  case class DownsamplingToSize(n: Int) extends Sampler {

    override def apply[T <: Any](arg: Sampler.In[T]): Out[T] = {
      Random
        .shuffle(arg)
        .slice(0, n)
    }
  }
}
