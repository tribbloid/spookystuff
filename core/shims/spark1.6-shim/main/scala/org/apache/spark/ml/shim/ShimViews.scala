package org.apache.spark.ml.shim

import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}

import scala.language.implicitConversions

object ShimViews {

  implicit class ParamsView(params: Params) {
    def trySetInputCols(v: Seq[Any]): Params = {
      params match {
        case s: HasInputCol =>
          require(v.size == 1, s"${s.getClass.getSimpleName} can only have 1 inputCol")
          s.set(s.inputCol, v.head)
        case ss: HasInputCols =>
          ss.set(ss.inputCols, v.toArray)
        case _ =>
          params
      }
    }

    def trySetOutputCol(v: Any): Params = {
      params match {
        case s: HasOutputCol =>
          s.set(s.outputCol, v)
        case _ =>
          params
      }
    }
  }

}