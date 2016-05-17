package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvSuite
import org.apache.spark.SparkException

case class Whatever(str: String)
case class WhateverAgain(str: String) extends NOTSerializableMixinSuite

class NOTSerializableMixinSuite extends SpookyEnvSuite {

  test("case class is serializable") {
    val whatever = new Whatever("abc")

    val in = 1 to 2
    val out = sc.parallelize(in).map{
      v =>
        whatever.str + v
    }.collect().toSeq

    assert(out == Seq("abc1", "abc2"))
  }

  test("case class with NotSerializableMixin will trigger a runtime error in closure cleaning") {
    val whatever = new WhateverAgain("abc")

    val in = 1 to 2
    intercept[SparkException] {
      sc.parallelize(in).map{
        v =>
          whatever.str + v
      }
    }
  }
}
