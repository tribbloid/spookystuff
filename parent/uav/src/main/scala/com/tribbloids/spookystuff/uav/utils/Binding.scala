package com.tribbloids.spookystuff.uav.utils

import java.util.UUID

import com.tribbloids.spookystuff.utils.IDMixin
import com.tribbloids.spookystuff.utils.lifespan.LifespanContext

import scala.util.Try

object Binding {

  val LOCK_EXPIRE_AFTER: Int = 60 * 1000 // TODO: de-hardcode?

  object Open extends Binding(None, LifespanContext(), 0) {

    override def toString: String = "(open)"

    override def getAvailability(keyOpt: Option[Binding]): Int = 0
  }

  def Transient(
      _id: Option[UUID] = Some(UUID.randomUUID()), // can only be lifted by PreferUAV that has the same token.
      ctx: LifespanContext = LifespanContext()
  ): Binding = Binding(_id, ctx, 0)

  def OnHold(
      _id: Option[UUID] = Some(UUID.randomUUID()), // can only be lifted by PreferUAV that has the same token.
      ctx: LifespanContext = LifespanContext()
  ): Binding = Binding(_id, ctx, System.currentTimeMillis() + LOCK_EXPIRE_AFTER)
}

/**
  * VERY IMPORTANT in attaching telemetry links to Spark tasks or Java threads a binded link cannot be commissioned for
  * anything else unless:
  *   - unlocked OR ALL OF THE FOLLOWING conditions are fulfilled:
  *   - expired after predefined timestamp
  *   - LifespanContext has completed OR a valid key is provided, the key either:
  *   - contains identical id OR
  *   - contain identical threadID (in this case, either in the same task or previous task that use it has completed)
  */
case class Binding(
    _id: Option[UUID], // can only be lifted by PreferUAV that has the same token.
    ctx: LifespanContext,
    expireAfter: Long
) extends IDMixin {

  def timeMillisLeft: Long = expireAfter - System.currentTimeMillis()

  def isExpired: Boolean = (timeMillisLeft < 0) && ctx.isCompleted

  override def toString: String = {
    val leftSecStr =
      if (isExpired) "expired"
      else if (expireAfter == Long.MaxValue) "permanent"
      else if (!ctx.isCompleted) s"possessed by task/thread"
      else "" + timeMillisLeft + "ms left"

    s"${ctx.toString} ($leftSecStr)${_id.map(" (" + _ + ")").getOrElse("")}"
  }

  /**
    * @param keyOpt
    *   access will be granted if the key contains identical UUID or threadID
    * @return
    *   strength of the availability, >0 means available, higher number means more preferable: -1 access denied 0
    *   binding has expired / open to all 1 on top of 1, key has identical threadID 2 key has identical taskID 3 key has
    *   identical ID (strongest)
    */
  def getAvailability(keyOpt: Option[Binding] = None): Int = {

    for (key <- keyOpt) {
      if (Try(this._id.get == key._id.get).getOrElse(false)) return 3
      else if (Try(this.ctx.taskAttemptID.get == key.ctx.taskAttemptID.get).getOrElse(false)) return 2
      // since 1 thread cannot run multiple tasks at the same time,
      // having identical threadID but different taskAttemptID implies that previous task has completed
      else if (this.ctx.threadID == key.ctx.threadID) return 1
    }

    if (isExpired) 0
    else -1
  }
}
