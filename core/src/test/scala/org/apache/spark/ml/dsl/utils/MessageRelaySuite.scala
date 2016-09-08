package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.dsl.AbstractFlowSuite

/**
  * Created by peng on 06/05/16.
  */
class MessageRelaySuite extends AbstractFlowSuite {

  test("SerializingParam[Function1] should work") {
    val fn = {k: Int => 2*k}
    val reader = new MessageReader[Int => Int]
    val param: reader.Param = reader.Param("id", "name", "")

    val json = param.jsonEncode(fn)
    println(json)
    val fn2 = param.jsonDecode(json)
    assert(fn2(2) == 4)
  }
}
