package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.testutils.FunSpecx

/**
  * Created by peng on 10/04/16.
  */
class DSLUtilsSuite extends FunSpecx {

  def get1(): Array[StackTraceElement] = {
    DSLUtils.getBreakpointInfo()
  }

  lazy val get2: Array[StackTraceElement] = get1()

  val get3: Array[StackTraceElement] = get2

  def defaultParamCaller(
      c: Array[StackTraceElement] = get2
  ): Array[StackTraceElement] = c

  it("methodName should return caller's name") {
    assert(get3.head.getMethodName == "get1")
    assert(get3(1).getMethodName == "get2")
    assert(get3(2).isNativeMethod)

    val dpc = defaultParamCaller()
    assert(dpc.head.getMethodName == "get1")
    assert(dpc.apply(1).getMethodName == "get2")
  }
}
