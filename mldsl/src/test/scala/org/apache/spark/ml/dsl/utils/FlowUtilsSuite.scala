package org.apache.spark.ml.dsl.utils

import org.scalatest.FunSuite

/**
  * Created by peng on 10/04/16.
  */
class FlowUtilsSuite extends FunSuite {

  def caller(): Array[StackTraceElement] = {
    FlowUtils.getBreakpointInfo()
  }

  lazy val caller2 = caller()

  val caller3 = caller2

  def defaultParamCaller(
                          c: Array[StackTraceElement] = caller()
                        ): Array[StackTraceElement] = c

  test("methodName should return caller's name") {
    assert(caller3.head.getMethodName == "caller")
    assert(caller3(1).getMethodName == "caller2")
    assert(caller3(2).isNativeMethod)

    val dpc = defaultParamCaller()
    assert(dpc.head.getMethodName == "caller")
    assert(dpc.apply(1).getMethodName == "defaultParamCaller$default$1")
  }
}