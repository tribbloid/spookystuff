//package com.tribbloids.spookystuff.conf
//
//import ai.acyclic.prover.commons.util.Caching
//
//// TODO: remove, merge into HasMorphism
//trait ParametricPoly1 extends GenParametricPoly1 {
//
//  type In[T <: UB] = T
//}
//
//object ParametricPoly1 {
//
//  trait Cached extends ParametricPoly1 {
//
//    lazy val cache: Caching.ConcurrentMap[UB, Out[_ <: UB]] = Caching.ConcurrentMap()
//
//    def get[T <: UB](k: T): Option[Out[T]] = {
//      cache
//        .get(k)
//        .map(v => v.asInstanceOf[Out[T]])
//    }
//
//    def getOrCompute[T <: UB](k: T): Out[T] = {
//      get(k)
//        .getOrElse {
//          val result = compute(k)
//          cache += k -> result
//          result
//        }
//    }
//
//    override def apply[T <: UB](k: T): Out[T] = { getOrCompute(k) }
//  }
//
//  // quite useless at this moment
//  trait CachedMutable extends Cached {
//
//    def update[T <: UB](k: T, v: Out[T]): Unit = {
//
//      cache += k -> v
//    }
//  }
//
//}
