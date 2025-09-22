package ai.acyclic.prover.commons.pending

import ai.acyclic.prover.commons.pending.PendingEffect.<<
import ai.acyclic.prover.commons.testlib.BaseSpec

class PendingEffectSpec extends BaseSpec {

  it("revokeAll") {

    // none of these works
//    val _: Ex = revokeAll(ex1)
//    val _: Ex = revokeAll(ex12)
//    val _: Ex = revokeAll(ex123)
  }

  it("revoke") {}

//  describe("Maybe") {
//
//    it("as the only effect") {
//
//      {
//        val v = "abc" <<: Maybe
//        val vv = v
//
//        Maybe._ops[String](v)
//
//        implicitly[v.type <:< Maybe[Maybe[String]]]
//        Maybe._ops[String](v) // this should fail
//
//        val o1: Option[String] = v.asOption
//        val o2: Option[String] = vv.asOption
//
//        assert(vv.asOption.contains("abc"))
//      }
//
//      {
//        val v: Maybe[String] = null <<: Maybe
//        assert(v.asOption.isEmpty)
//      }
//    }
//
//    it("... for primitive type") {
//
//      {
//        val v = 1 <<: Maybe
//        val o1: Option[Int] = v.asOption
//        assert(v.asOption.contains(1))
//      }
//
//      {
//        val v: Maybe[Int] = Maybe.unset[Int]
//        assert(v.asOption.isEmpty)
//      }
//    }
//  }

//  describe("MayThrow") {
//
//    it("can execute as the only effect") {
//
//      {
//        val v = (() => "abc") <<: MayThrow[UnsupportedOperationException]
//        val vv = v
//        assert(vv.asEither.toOption.contains("abc"))
//      }
//
//      {
//        val fn: () => String = { () =>
//          throw new UnsupportedOperationException()
//        }
//
//        val v = fn <<: MayThrow[UnsupportedOperationException]
//        assert(v.asEither.toOption.isEmpty)
//      }
//    }
//  }
}

object PendingEffectSpec extends PendingEffect.Universe {

  trait IO1 extends PendingEffect {}

//  object IO1 {}

  trait IO2 extends PendingEffect {}

  trait IO3 extends PendingEffect {}

  trait Ex {

    def fn(v: Int): Int
  }

  //  trait Ext[C <: Subject.Capability] extends Ex {}

  val ex0: Ex = { v => v + 1 }

  val ex1: Ex << IO1 = ex0

//  val ex12: Ex << IO1 << IO2 = ex1 // only works in Scala 3
  val ex12: Ex << (IO1 & IO2) = ex1

  val ex123: Ex << (IO1 & IO2 & IO3) = ex12
}
