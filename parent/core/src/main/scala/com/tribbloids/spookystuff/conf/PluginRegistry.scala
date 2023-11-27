package com.tribbloids.spookystuff.conf

import ai.acyclic.prover.commons.Same
import ai.acyclic.prover.commons.function.PreDef
import ai.acyclic.prover.commons.function.PreDef.MorphismOps
import ai.acyclic.prover.commons.util.Caching
import com.tribbloids.spookystuff.utils.TreeThrowable
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable

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
    trait Dependent extends PreDef.Dependent[P] { type Out[T <: P] = Factory.this.Out[T] }

    protected def init: Dependent

    final lazy val cached: PreDef.Morphism.Cached[P, Dependent] =
      new MorphismOps[P, Dependent](init).cachedBy(
        Same.ByEquality.Lookup(Caching.ConcurrentMap())
      )

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

    implicit def asMorphism(v: this.type): cached.type = v.cached
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
