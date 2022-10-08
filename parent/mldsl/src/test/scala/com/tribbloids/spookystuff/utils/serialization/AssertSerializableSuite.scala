package com.tribbloids.spookystuff.utils.serialization

import com.tribbloids.spookystuff.testutils.FunSpecx

import scala.util.Try

class AssertSerializableSuite extends FunSpecx {

  it("IllegalArgumentException should be WeaklySerializable") {

    val trial = Try {
      require(
        requirement = false,
        "error!"
      )
    }
    val ee = trial.failed.get

//    TestHelper.TestSC.parallelize(Seq(ee))
//      .collect() //TODO: this failed, why?

    AssertWeaklySerializable(ee)
  }
}