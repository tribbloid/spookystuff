package com.tribbloids.spookystuff.commons.serialization

import ai.acyclic.prover.commons.function.hom.Hom
import ai.acyclic.prover.commons.util.Caching
import com.tribbloids.spookystuff.testutils.BaseSpec
import com.twitter.chill.ClosureCleaner

import scala.util.Try

class AssertSerializableSpike extends BaseSpec {
  import AssertSerializableSpike._

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

    describe("by ClosureCleaner") {

      it("0") {

        AssertWeaklySerializable(Outer.inner0)
      }

      it("1") {

        AssertWeaklySerializable(Outer.inner1)
      }

      it("2") {

        AssertWeaklySerializable(Outer.inner2)
      }

      it("3") {

        AssertWeaklySerializable(Outer.inner3)
      }
    }
  }

  describe("should be Serializable with equality - ") {

    typeOfIt {
      (): Unit
    } { v =>
      AssertWeaklySerializable(v)
    }
  }

  ignore("not working") {

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
      AssertWeaklySerializable[Caching.Strong.Impl[String, Int]](
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
      val v = Caching.Weak.Impl[String, Int]()
      v.put("a", 1)
      v
    } { v =>
      AssertWeaklySerializable(v)
    }
  }

}

case object AssertSerializableSpike {

  trait Fn[-I, +O] extends (I => O) {}

  object Outer extends NOTSerializable {

    // everything here should be extracted safely by Spark Closure cleaner

    val inner0: String => Int = { (_: String) =>
      3
    }

    val inner1: Fn[String, Int] = new Fn[String, Int] {
      override def apply(v1: String): Int = 3
    }

    val inner2: Hom.Impl.Circuit[String, Int] = Hom.Circuit { _ =>
      3
    }

    val inner3: String => Int = inner0
  }
}
