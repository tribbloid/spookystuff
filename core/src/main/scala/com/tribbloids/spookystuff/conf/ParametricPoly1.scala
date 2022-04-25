package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.utils.CachingUtils

trait ParametricPoly1 extends GenParametricPoly1 {

  type In[T <: UB] = T
}

object ParametricPoly1 {

  trait Cached extends ParametricPoly1 {

    lazy val cache: CachingUtils.ConcurrentMap[UB, Out[_ <: UB]] = CachingUtils.ConcurrentMap()

    def get[T <: UB](k: T): Option[Out[T]] = {
      cache
        .get(k)
        .map(v => v.asInstanceOf[Out[T]])
    }

    def getOrCompute[T <: UB](k: T): Out[T] = {
      get(k)
        .getOrElse {
          val result = compute(k)
          cache += k -> result
          result
        }
    }

    override def apply[T <: UB](k: T): Out[T] = { getOrCompute(k) }
  }

  // quite useless at this moment
  trait CachedMutable extends Cached {

    def update[T <: UB](k: T, v: Out[T]): Unit = {

      cache += k -> v
    }

//    trait FlatTransformer extends GenParametricPoly1 {
//      override type UB = CachedMutable.this.UB
//      override type In[T <: UB] = (T, CachedMutable.this.Out[T])
//      override type Out[T <: UB] = Option[CachedMutable.this.Out[T]]
//    }
//
//    def flatTransform(fn: FlatTransformer): Unit = {
//
//      val existing = cache.toSeq
//      existing.foreach { kv =>
//        val k = kv._1
//
//        val _kv = kv.asInstanceOf[(k.type, Out[k.type])]
//
//        val opt: Option[Out[k.type]] = fn.apply[k.type](_kv)
//        opt.foreach { transformed =>
//          cache.put(_kv._1, transformed)
//        }
//      }
//    }
  }

}
