package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.IDMixin
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.BatchID
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
    registeredID
  }

  def requireUsable(): Unit = {
    require(isInitialised != null, s"$this not initialised")
  }

  def ctxFactory: () => LifespanContext
  @transient lazy val ctx: LifespanContext = ctxFactory()

  protected def _register: Seq[BatchID]
  @transient final lazy val registeredID = _register
  final def _id: Seq[BatchID] = registeredID

  def nameOpt: Option[String]
  override def toString: String = {
    val idStr = Try(registeredID.mkString("/")).getOrElse("[Error]")
    (nameOpt.toSeq ++ Seq(idStr)).mkString(":")
  }
}
