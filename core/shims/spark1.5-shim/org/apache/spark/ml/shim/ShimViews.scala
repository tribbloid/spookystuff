package org.apache.spark.ml.shim

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}

import scala.language.implicitConversions

object ShimViews {

  implicit class ParamsView(params: Params) {
    val setter = params.getClass.getMethod("set", classOf[Param[_]], classOf[AnyRef])

    def trySetInputCols(v: Seq[String]): Params = {
      params match {
        case s: HasInputCol =>
          require(v.size == 1, s"${s.getClass.getSimpleName} can only have 1 inputCol")
          setter.invoke(params, s.inputCol, v.head)
          params
        case s: HasInputCols =>
          setter.invoke(params, s.inputCols, v.toArray[String])
          params
        case _ =>
          params
      }
    }

    def trySetOutputCol(v: String): Params = {
      params match {
        case s: HasOutputCol =>
          setter.invoke(params, s.outputCol, v)
          params
        case _ =>
          params
      }
    }
  }
}

