package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.dsl.UDFTransformer
import org.apache.spark.ml.feature.IDF
import org.apache.spark.sql.functions.udf
import org.scalatest.FunSpec

/**
  * Created by peng on 10/04/16.
  */
class DSLUtilsSuite extends FunSpec {

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

  it("IDF can be replicated") {

    val v1 = new IDF().setInputCol("a").setOutputCol("b")
    val v2 = DSLUtils.replicateStage(v1)

    def valuesOf(v: PipelineStage) = {
      v.paramMap.toSeq.map(_.value).toSet
    }

    assert(valuesOf(v1) == valuesOf(v2))
  }

  it("UDFTransformer can be replicated") {

    val stemming = udf { v: Seq[String] =>
      v.map(_.stripSuffix("$"))
    }
    val v1 = UDFTransformer().setUDFSafely(stemming).setInputCols(Array("name_token")).setOutputCol("name_stemmed")
    val v2 = DSLUtils.replicateStage(v1)

    assert(v1.udfImpl == v2.udfImpl)
  }
}
