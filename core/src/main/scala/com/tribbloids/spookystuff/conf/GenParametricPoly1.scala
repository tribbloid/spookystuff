package com.tribbloids.spookystuff.conf

trait GenParametricPoly1 {

  type UB

  type In[_ <: UB]
  type Out[_ <: UB]

  def compute[T <: UB](v: In[T]): Out[T]

  def apply[T <: UB](v: In[T]): Out[T] = compute(v)
}

object GenParametricPoly1 {}
