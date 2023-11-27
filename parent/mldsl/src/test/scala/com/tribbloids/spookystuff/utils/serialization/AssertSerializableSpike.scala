package com.tribbloids.spookystuff.utils.serialization

import ai.acyclic.prover.commons.util.Caching
import com.tribbloids.spookystuff.testutils.BaseSpec

import scala.util.Try

class AssertSerializableSpike extends BaseSpec {

  describe("should be WeaklySerializable - ") {

    classOfIt {

      val trial = Try {
        require(
          requirement = false,
          "error!"
        )
      }
      val ee = trial.failed.get
      ee
    } { v =>
      AssertWeaklySerializable(v)

      //    TestHelper.TestSC.parallelize(Seq(ee))
      //      .collect() //TODO: this failed, why?
    }

  }

  ignore("should be Serializable with equality - ") {

    typeOfIt {
      val cache = Caching.Strong.underlyingBuilder.build[String, Int]()
      cache.put("A", 1)
      cache
    } { v =>
      AssertWeaklySerializable(v)
    }

    typeOfIt {
      val v = Caching.Strong.build[String, Int]()
      v.put("a", 1)
      v
    } { v =>
      AssertWeaklySerializable[Caching.Strong.View[String, Int]](
        v,
        condition = { (v1, v2) =>
          v1 == v2
        }
      )
    }

    typeOfIt {
      val v = Caching.Soft.build[String, Int]()
      v.put("a", 1)
      v
    } { v =>
      AssertWeaklySerializable(v)

    }

    typeOfIt {
      val v = Caching.Weak.View[String, Int]()
      v.put("a", 1)
      v
    } { v =>
      AssertWeaklySerializable(v)
    }
  }

}
