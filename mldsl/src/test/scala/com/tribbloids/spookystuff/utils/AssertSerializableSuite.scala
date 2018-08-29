package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.TestHelper
import org.scalatest.FunSpec

import scala.util.Try

class AssertSerializableSuite extends FunSpec {

  it("IllegalArgumentException should be WeaklySerializable"){

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
