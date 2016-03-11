//package com.tribbloids.spookystuff.utils
//
//import scala.language.implicitConversions
//
//class Lazy[T](v: => T) extends Serializable {
//  private var state: Option[T] = None
//
//  def value: T = if (state.isDefined) state.get else {
//    state = Some(v)
//    state.get
//  }
//
//  def reset() {
//    state = None
//  }
//}
//
//object Lazy {
//  def apply[T](v: => T) = new Lazy[T](v)
//}