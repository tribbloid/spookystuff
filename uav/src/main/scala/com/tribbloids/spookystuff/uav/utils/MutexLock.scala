package com.tribbloids.spookystuff.uav.utils

import com.tribbloids.spookystuff.utils.IDMixin

import scala.util.Random

object MutexLock {

  val LOCK_EXPIRE_AFTER = 60 * 1000 //TODO: de-hardcod?
}

case class MutexLock(
                      _id: Long = Random.nextLong(), //can only be lifted by PreferUAV that has the same token.
                      timestamp: Long = System.currentTimeMillis()
                    ) extends IDMixin {

  def expireAfter = timestamp + MutexLock.LOCK_EXPIRE_AFTER
}