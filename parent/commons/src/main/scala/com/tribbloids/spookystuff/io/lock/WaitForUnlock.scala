package com.tribbloids.spookystuff.io.lock

import com.tribbloids.spookystuff.io.{URIExecution, URIResolver}

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

case class WaitForUnlock(
    exe: URIExecution,
    expired: LockExpired = URIResolver.default.lockExpired
) extends LockLike {

  def unlockIfPossible(): Unit = {

    try {
      exe.input { ii =>
        ii.getType
      }
    } catch {
      case ee @ (_: FileNotFoundException | _: NoSuchFileException) =>
        val canBeUnlocked = expired.scanForUnlocking(Moved.dir)

        canBeUnlocked match {
          case Some(v) =>
            v.exe.moveTo(exe.absolutePathStr)
          case None =>
            throw ee
        }
    }
  }

  def duringOnce[T](fn: URIExecution => T): T = {
    unlockIfPossible()

//    try {
    fn(exe)
//    }
  }

  final def during[T](fn: URIExecution => T): T = inMemory.synchronized {
    resolver.retry {
      duringOnce(fn)
    }
  }
}
