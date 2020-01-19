package com.tribbloids.spookystuff.utils.io.lock

import com.tribbloids.spookystuff.utils.BypassingRule
import com.tribbloids.spookystuff.utils.io.{URIExecution, URIResolver}

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

case class WaitForUnlock(
    source: URIExecution,
    expired: LockExpired = URIResolver.default.expired
) extends LockLike {

  def unlockIfPossible(): Unit = {

    try {
      source.input { ii =>
        ii.getType
      }
    } catch {
      case ee @ (_: FileNotFoundException | _: NoSuchFileException) =>
        val canBeUnlocked = expired.scanForUnlocking(Moved.dir)

        canBeUnlocked match {
          case Some(v) =>
            v.exe.moveTo(source.absolutePathStr)
          case None =>
            throw ee
        }
    }
  }

  def duringOnce[T](fn: URIExecution => T): T = {
    unlockIfPossible()

    try {
      fn(source)
    } catch {
      case e: Lock.CanReattempt =>
        throw e
      case e @ _ =>
        throw BypassingRule.NoRetry(e)
    }
  }

  final def during[T](fn: URIExecution => T): T = {
    resolver.retry {
      duringOnce(fn)
    }
  }
}
