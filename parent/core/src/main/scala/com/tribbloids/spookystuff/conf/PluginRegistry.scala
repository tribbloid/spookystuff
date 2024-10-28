package com.tribbloids.spookystuff.conf

import ai.acyclic.prover.commons.function.hom.Hom
import ai.acyclic.prover.commons.same.Same
import ai.acyclic.prover.commons.util.Caching
import com.tribbloids.spookystuff.commons.TreeThrowable
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable

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
      ubEv: ClassTag[P]
  ) extends Serializable {

    type Out[_ <: P]
    trait Impl extends Hom.Impl.Dependent[P, Out]

    protected def init: Impl

    final lazy val cached: Hom.Mono.Cached[P, Impl] = {

      Hom
        .MonoOps(init)
        .cached(
          Same.Native.Lookup(Caching.ConcurrentMap())
        )
    }

    {
      cached
    }

    def registerEnabled(): Unit = {
      // ahead-of-time initialization based on enabled plugins
      // may take a long time then fail, only attempted once
      val trials = enabled.flatMap {
        case v: P =>
          val result = scala.util.Try {
            this.apply(v)
          }
          Some(result)
        case _ =>
          None
      }.toSeq

      TreeThrowable.&&&(trials)
    }

    implicit def asMono(v: this.type): cached.type = v.cached
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
