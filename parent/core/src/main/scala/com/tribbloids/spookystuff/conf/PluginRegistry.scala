package com.tribbloids.spookystuff.conf

import ai.acyclic.prover.commons.function.bound.TypeBound
import ai.acyclic.prover.commons.function.hom.Hom.BoundView
import ai.acyclic.prover.commons.multiverse.CanEqual
import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.Caching
import com.tribbloids.spookystuff.commons.TreeThrowable

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

trait PluginRegistry {

  type Plugin

  @transient lazy val enabled: ArrayBuffer[Plugin] = ArrayBuffer.empty

  def enable(k: Plugin): Unit =
    if (!enabled.contains(k))
      enabled += k

  def disable(k: Plugin): Unit = enabled -= k

  abstract class Factory[P <: Plugin](
      implicit
      ctg: ClassTag[P]
  ) extends Serializable {
    {
      cached
    }

    object Bound extends TypeBound {

      type Min = Nothing
      type Max = P

      val view = new BoundView(this)
    }

    type Out[_ <: P]
    trait Impl extends Bound.view.Dependent.Impl[Out]

    protected def init: Impl

    final lazy val cached = {

      init
        .cached(
          CanEqual.Native.Lookup(Caching.ConcurrentMap())
        )
    }

    def registerEnabled(): Unit = {
      // ahead-of-time initialization based on enabled plugins
      // may take a long time then fail, only attempted once
      val trials = enabled.flatMap {
        case v: P =>
          val result = scala.util.Try {
            cached.apply(v)
          }
          Some(result)
        case _ =>
          None
      }.toSeq

      TreeThrowable.&&&(trials)
    }

    implicit def asCached(v: this.type): cached.type = v.cached
  }
}

object PluginRegistry extends PluginRegistry with NOTSerializable {

  type Plugin = PluginSystem

  {
    Core.enableOnce
    Dir.enableOnce
    Python.enableOnce
  }
}
