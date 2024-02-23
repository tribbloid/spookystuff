package com.tribbloids.spookystuff.commons.lifespan

case class ThreadLocal[A](init: LifespanContext => A) extends java.lang.ThreadLocal[A] {

  override def initialValue: A = {
    val ctx = LifespanContext()
    init(ctx)
  }

//  def apply: A = get
}
