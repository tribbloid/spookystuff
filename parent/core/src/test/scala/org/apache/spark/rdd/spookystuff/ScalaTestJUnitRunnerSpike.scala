package org.apache.spark.rdd.spookystuff

import org.scalatest.Suite
import org.scalatest.funspec.AnyFunSpec

import scala.collection.immutable

class ScalaTestJUnitRunnerSpike extends AnyFunSpec {

  case class Child(override val suiteName: String = "Child") extends AnyFunSpec {

    it("test 1") {}
  }

  object C1 extends Child()
  object C2 extends Child()

  override val nestedSuites: immutable.IndexedSeq[Suite] = {

//    immutable.IndexedSeq(Child(), Child()) // will trigger runner error due to duplicate names
    immutable.IndexedSeq(C1, C2) // will not
//    immutable.IndexedSeq(Child("N1"), Child("N2")) // same error, what's the cache?
    immutable.IndexedSeq(new Child() {}, new Child() {}) // will not, because they are anon classes
  }
}
