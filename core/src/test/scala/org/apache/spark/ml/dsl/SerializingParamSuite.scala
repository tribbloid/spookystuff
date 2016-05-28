package org.apache.spark.ml.dsl

/**
  * Created by peng on 06/05/16.
  */
class SerializingParamSuite extends AbstractFlowSuite {

  test("SerializingParam[Function1] should work") {
    val fn = {k: Int => 2*k}
    val param = new JavaSerializationParam[Int => Int]("id", "name", "")

    val json = param.jsonEncode(fn)
    println(json)
    val fn2 = param.jsonDecode(json)
    assert(fn2(2) == 4)
  }
}
