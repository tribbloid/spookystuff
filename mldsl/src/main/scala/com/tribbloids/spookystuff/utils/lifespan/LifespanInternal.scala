package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.IDMixin
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.{Batch, BatchID}
import com.tribbloids.spookystuff.utils.serialization.BeforeAndAfterShipping

import scala.util.Try

abstract class LifespanInternal extends BeforeAndAfterShipping with IDMixin {

  {
    initOnce
  }

  override def afterArrival(): Unit = {
    initOnce
  }

  @transient private final var isInitialised: LifespanInternal = _
  @transient protected final lazy val initOnce: Unit = {
    //always generate on construction or deserialization

    doInit()
    isInitialised = this
  }

  protected def doInit(): Unit = {
    ctx
    registeredBatches
  }

  def requireUsable(): Unit = {
    require(isInitialised != null, s"$this not initialised")
  }

  def ctxFactory: () => LifespanContext
  @transient lazy val ctx: LifespanContext = ctxFactory()

  def children: List[LeafType#Internal] = Nil

  @transient final lazy val leaves: Seq[LeafType#Internal] = this match {
    case v: LeafType#Internal =>
      Seq(v) ++ children
    case _ =>
      children
  }

  protected def _registerBatches_CleanSweepHooks: Seq[(BatchID, Batch)]
  @transient final lazy val registeredBatches = _registerBatches_CleanSweepHooks
  final lazy val registeredIDs: Seq[BatchID] = registeredBatches.map(v => v._1)
  final protected def _id: Seq[BatchID] = registeredIDs

  def nameOpt: Option[String]
  override def toString: String = {
    val idStr = Try(registeredIDs.mkString("/")).getOrElse("[Error]")
    (nameOpt.toSeq ++ Seq(idStr)).mkString(":")
  }
}
