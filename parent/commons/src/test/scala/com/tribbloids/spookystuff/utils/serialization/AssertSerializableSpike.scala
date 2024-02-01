package com.tribbloids.spookystuff.utils.serialization

import ai.acyclic.prover.commons.function.Impl
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

    describe("inner closure of an object that is not serializable") {

      it("vanilla function") {

        AssertWeaklySerializable(Outer.inner1)
      }

      it("Fn by single method interface") {

        AssertWeaklySerializable(Outer.inner2)
      }

      it("Fn by conversion") {

        AssertWeaklySerializable(Outer.inner3)
      }
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

  object Outer extends NOTSerializable {

    val inner1: String => Int = { _: String =>
      3
    }

    val inner2: Impl.Fn[String, Int] = Impl { _ =>
      3
    }

    val inner3: String => Int = inner1
  }
}

object AssertSerializableSpike {}
