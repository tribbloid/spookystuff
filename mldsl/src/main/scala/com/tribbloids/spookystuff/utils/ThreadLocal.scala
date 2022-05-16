package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.lifespan.LifespanContext

case class ThreadLocal[A](init: LifespanContext => A) extends java.lang.ThreadLocal[A] {

  override def initialValue: A = {
    val ctx = LifespanContext()
    init(ctx)
  }

//  def apply: A = get
}
