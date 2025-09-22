package com.tribbloids.spookystuff.spike

import org.scalatest.funspec.AnyFunSpec

class ScalaTestJUnitRunnerSpike extends AnyFunSpec {

  case class Child(override val suiteName: String = "Child") extends AnyFunSpec {

    it("test 1") {}
  }

  object C1 extends Child()
  object C2 extends Child()

  override lazy val nestedSuites: Vector[Child] = {

//    Vector(Child(), Child()) // will trigger runner error due to duplicate names
    Vector(C1, C2) // will not
//    Vector(Child("N1"), Child("N2")) // same error, what's the cache?
    Vector(new Child() {}, new Child() {}) // will not, because they are anon classes
  }
}
