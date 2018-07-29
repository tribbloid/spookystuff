package com.tribbloids.spookystuff.utils

import java.io.NotSerializableException

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.testutils.AssertSerializable
import org.apache.spark.SparkException

case class Thing(str: String)
case class Thing2(str: String) extends NOTSerializableMixinSuite

class NOTSerializableMixinSuite extends SpookyEnvFixture {

  it("case class is serializable") {
    val whatever = Thing("abc")

    AssertSerializable(whatever)

    val in = 1 to 2
    val out = sc.parallelize(in).map{
      v =>
        whatever.str + v
    }.collect().toSeq

    assert(out == Seq("abc1", "abc2"))
  }

  it("case class with NotSerializableMixin will trigger a runtime error in closure cleaning") {
    val whatever = Thing2("abc")

    intercept[NotSerializableException]{
      AssertSerializable(whatever)
    }

    val in = 1 to 2
    intercept[SparkException] {
      sc.parallelize(in).map{
        v =>
          whatever.str + v
      }
    }
  }
}
