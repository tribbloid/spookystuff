package com.tribbloids.spookystuff.frameless

import scala.language.implicitConversions

object OverloadPriority {

//  trait Lvl1 {
//    def f(a: Int, b: Int) = 1
//  }
//
//  class C extends Lvl1 {
//    def f() = 2
//  }
//  object Test extends App {
//    val c = new C()
//    println(c.f(b = 2, 2))
//    println(c.f(a = 2, c = 2))
//    println(c.f(2, c = 2))
//    println(c.f(c = 2, 2))
//    println(c.f(2))
//  }

  //  trait Lvl1 {}
//  object OverloadedApply extends Lvl1 {
//    def apply(x: Int)(y: Int): String = s"Int: $x, $y"
//    def apply(x: Int)(y: Double): String = s"Int-Double: $x, $y"
//  }
//
//  def main(args: Array[String]): Unit = {
//    val result = OverloadedApply.apply(1)(2.0)
//    println(result)
//  }

//
//  trait A
//
//  object A {
//
//    implicit def asSeq(a: A): Seq[A] = Seq(a)
//  }
//
////  trait B
////  trait C extends A with B
//
//  trait LowLevel {
//
////    def f[T, R](x: T): String = "A"
//  }
//
//  object HighLevel extends LowLevel {
//
//    def f[T](x: T): String = "A"
//
//    def f[T](x: Seq[T]): String = "B"
//
//    def f[T, R](x: T)(
//        implicit
//        ev: T <:< Seq[R]
//    ): String = "C"
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val s = HighLevel.f(new A {}) // Vector(C@1)
//    println(s)
//  }
}
