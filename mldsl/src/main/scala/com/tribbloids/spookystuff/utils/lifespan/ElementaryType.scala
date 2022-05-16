package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.BatchID
import com.tribbloids.spookystuff.utils.serialization.BeforeAndAfterShipping

abstract class LifespanType extends Serializable with Product {

  type ID
}

abstract class ElementaryType extends LifespanType {

  protected def _batchID(ctx: LifespanContext): ID
  protected def _registerHook(ctx: LifespanContext, fn: () => Unit): Unit

  class Elementary(
      val nameOpt: Option[String] = None,
      val ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends LifespanInternal
      with BeforeAndAfterShipping {

    def _type: ElementaryType = ElementaryType.this

    @transient lazy val batchID: ID = _type._batchID(ctx).asInstanceOf[ID]

    def registerHook(fn: () => Unit): Unit = {
      _type._registerHook(ctx, fn)
    }

    final override protected def _register: Seq[BatchID] = {

      val batchID = this.batchID

      Cleanable.Select(batchID).getOrExecute { () =>
        registerHook { () =>
          Cleanable.Select(batchID).cleanSweep()
        }
        ConcurrentMap()
      }

      Seq(batchID)
    }
  }

  def apply(
      nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ): BeforeAndAfterShipping.Container[Elementary] = {
    val i = new Elementary(nameOpt, ctxFactory)
    i.forShipping
  }
}
