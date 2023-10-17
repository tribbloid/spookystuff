package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.BatchInstances

abstract class LeafType extends LifespanType {

  protected def _batchID(ctx: LifespanContext): ID
  protected def _registerHook(ctx: LifespanContext, fn: () => Unit): Unit

  class Internal(
      val nameOpt: Option[String] = None,
      val ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends LifespanInternal {

    def _type: LeafType = LeafType.this

    @transient lazy val batchID: ID = _type._batchID(ctx).asInstanceOf[ID]

    def registerHook(fn: () => Unit): Unit = {
      _type._registerHook(ctx, fn)
    }

    final override protected def _registerBatches_CleanSweepHooks: Seq[(ID, BatchInstances)] = {

      val batchID = this.batchID

      def select = Cleanable.Batch(batchID)

      // if the batch for this batchID already existed, it means the cleanSweep hook is already registered
      val batch = select.getOrExecute { () =>
        registerHook { () =>
          select.cleanSweep()
        }
        ConcurrentMap()
      }

      Seq(batchID -> batch)
    }
  }

  def apply(
      nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ): Internal#ForShipping = {
    val i = new Internal(nameOpt, ctxFactory)
    i.forShipping
  }
}
