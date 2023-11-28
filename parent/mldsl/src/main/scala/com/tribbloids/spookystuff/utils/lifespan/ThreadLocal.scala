package com.tribbloids.spookystuff.utils.lifespan

case class ThreadLocal[A](init: LifespanContext => A) extends java.lang.ThreadLocal[A] {

  override def initialValue: A = {
    val ctx = LifespanContext()
    init(ctx)
  }

//  def apply: A = get
}
