package com.tribbloids.spookystuff.relay

import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.relay.TestBeans._

class RelayRegistrySuite extends BaseSpec {

  describe("lookup") {

    it("can find Relay as companion object") {

      val v = Multipart("a", "b")()
      val rr = RelayRegistry.Default.lookupFor(v)
      assert(rr == Multipart)

      val rr2 = RelayRegistry.Default.lookupFor(v)
      assert(rr2 == Multipart)
    }

    it("will throw an exception if companion object is not a Relay") {

      val v = User("a")
      intercept[UnsupportedOperationException] {
        RelayRegistry.Default.lookupFor(v)
      }
    }
  }

}
