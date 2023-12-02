package com.tribbloids.spookystuff.utils


trait Default_Imp0 {
  // Stop AnyRefs from clashing with AnyVals
  implicit def defaultNull[A <: AnyRef]: Default[A] = new Default[A](null.asInstanceOf[A])
}