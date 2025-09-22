package ai.acyclic.prover.commons.pending

import ai.acyclic.prover.commons.testlib.BaseSpec

class MayThrowSpec extends BaseSpec {

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
