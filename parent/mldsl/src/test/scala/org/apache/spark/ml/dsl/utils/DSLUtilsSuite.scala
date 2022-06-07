package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.testutils.FunSpecx

/**
  * Created by peng on 10/04/16.
  */
class DSLUtilsSuite extends FunSpecx {

  def caller(): Array[StackTraceElement] = {
    DSLUtils.getBreakpointInfo()
  }

  lazy val caller2: Array[StackTraceElement] = caller()

  val caller3: Array[StackTraceElement] = caller2

  def defaultParamCaller(
      c: Array[StackTraceElement] = caller2
  ): Array[StackTraceElement] = c

  it("methodName should return caller's name") {
    assert(caller3.head.getMethodName == "caller")
    assert(caller3(1).getMethodName == "caller2")
    assert(caller3(2).isNativeMethod)

    val dpc = defaultParamCaller()
    assert(dpc.head.getMethodName == "caller")
    assert(dpc.apply(1).getMethodName == "caller2")
  }
}
