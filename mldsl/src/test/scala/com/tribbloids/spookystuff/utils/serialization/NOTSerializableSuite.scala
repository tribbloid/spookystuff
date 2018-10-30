package com.tribbloids.spookystuff.utils.serialization

import java.io.NotSerializableException

import com.tribbloids.spookystuff.testutils.{FunSpecx, TestHelper}
import org.apache.spark.SparkException

case class Thing(str: String)
case class Thing2(str: String) extends NOTSerializable {

//  @throws(classOf[IOException])
//  private def writeObject(out: ObjectOutputStream): Unit = {
//    this.str
//    out.defaultWriteObject()
//  }
}

class NOTSerializableSuite extends FunSpecx {

  it("base class is serializable") {
    val thing = Thing("abc")

    AssertSerializable(thing)

    val in = 1 to 2
    val out = TestHelper.TestSC
      .parallelize(in)
      .map { v =>
        thing.str + v
      }
      .collect()
      .toSeq

    assert(out == Seq("abc1", "abc2"))
  }

  SerBox.serializers.foreach { ser =>
    it(s"mixin will trigger a runtime error if accessed by ${ser.getClass.getSimpleName}") {
      val thing = Thing2("abc")

      intercept[NotSerializableException] {
        AssertWeaklySerializable(thing, Seq(ser))
      }
    }
  }

  it("mxin will trigger a runtime error in closure cleaning") {
    val thing = Thing2("abc")

    val in = 1 to 2
    intercept[SparkException] {
      TestHelper.TestSC.parallelize(in).map { v =>
        thing.str + v
      }
    }
  }
}
