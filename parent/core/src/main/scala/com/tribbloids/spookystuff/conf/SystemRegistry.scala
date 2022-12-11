package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.utils.TreeThrowable

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait SystemRegistry {

  type _Sys

  lazy val enabled: ArrayBuffer[_Sys] = ArrayBuffer.empty

  def enable(v: _Sys): Unit =
    if (!enabled.contains(v))
      enabled += v

  def disable(v: _Sys): Unit = enabled -= v

  trait Factory extends ParametricPoly1.CachedMutable with Serializable {

    type UB <: _Sys
    implicit def ubEv: ClassTag[UB]

    @transient lazy val _outer: SystemRegistry.this.type = SystemRegistry.this

    def createEnabled(): Unit = {
      // ahead-of-time initialization based on parent
      // may take a long time then fail, only attempted once
      val trials = enabled
        .flatMap {
          case v: UB =>
            val result = scala.util.Try {
              apply(v)
            }
            Some(result)
          case _ =>
            None
        }

      TreeThrowable.&&&(trials)
    }
  }

  // useless at the moment
  trait AOTFactory extends Factory {

    {
      createEnabled()
    }
  }
}

object SystemRegistry {}
