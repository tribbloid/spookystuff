package com.tribbloids.spookystuff.uav.utils

import com.tribbloids.spookystuff.utils.IDMixin

import scala.util.Random

object Lock {

  val LOCK_EXPIRE_AFTER = 60 * 1000 //TODO: de-hardcod?
}

/**
  * VERY IMPORTANT in attaching telemetry links to Spark tasks or Java threads
  * a locked link cannot be commissioned for anything else unless:
  *   - unlocked
  * OR ALL OF THE FOLLOWING conditions are fulfilled:
  *   - lock is expired after a predefined duration
  *   - lock's LifespanContext has completed
  * OR:
  *   - 
  */
case class Lock(
                 _id: Long = Random.nextLong(), //can only be lifted by PreferUAV that has the same token.
                 timestamp: Long = System.currentTimeMillis()
               ) extends IDMixin {

  def expireAfter = timestamp + Lock.LOCK_EXPIRE_AFTER

}