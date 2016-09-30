package com.tribbloids.spookystuff.utils

import java.io.NotSerializableException

import com.tribbloids.spookystuff.SpookyEnvFixture
import org.apache.spark.SparkException

case class Thing(str: String)
case class Thing2(str: String) extends NOTSerializableMixinSuite

class NOTSerializableMixinSuite extends SpookyEnvFixture {

  test("case class is serializable") {
    val whatever = new Thing("abc")

    assertSerializable(whatever)

    val in = 1 to 2
    val out = sc.parallelize(in).map{
      v =>
        whatever.str + v
    }.collect().toSeq

    assert(out == Seq("abc1", "abc2"))
  }

  test("case class with NotSerializableMixin will trigger a runtime error in closure cleaning") {
    val whatever = new Thing2("abc")

    intercept[NotSerializableException]{
      assertSerializable(whatever)
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
