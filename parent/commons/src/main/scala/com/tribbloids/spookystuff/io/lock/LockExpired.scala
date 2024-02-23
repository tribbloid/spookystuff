package com.tribbloids.spookystuff.io.lock

import com.tribbloids.spookystuff.io.URIExecution

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException
import scala.concurrent.duration.Duration

case class LockExpired(
    unlockAfter: Duration,
    deleteAfter: Duration
) {

  case class ScanResult(exe: URIExecution) {

    val lastModified: Long = exe.input { in =>
      in.getLastModified
    }

    val elapsedMillis: Long = System.currentTimeMillis() - lastModified

    lazy val canBeUnlocked: Boolean = elapsedMillis >= unlockAfter.toMillis
    lazy val canBeDeleted: Boolean = canBeUnlocked && elapsedMillis >= deleteAfter.toMillis
  }

  protected def _scanForUnlocking(vs: Seq[URIExecution]): Option[ScanResult] = {

    val scanResult = vs.flatMap { v =>
      if (v.isExisting) {

        try {
          Some(ScanResult(v))
        } catch {
          case _: FileNotFoundException => None
          case _: NoSuchFileException   => None
        }
      } else {
        None
      }
    }

    val canBeUnlocked = scanResult.filter(_.canBeUnlocked)

    val latest = canBeUnlocked.sortBy(_.lastModified).lastOption

    val canBeDeleted = canBeUnlocked.filter(v => latest.contains(v)).filter(_.canBeDeleted)

    canBeDeleted.foreach { v =>
      v.exe.delete(false)
    }

    latest
  }

  def scanForUnlocking(lockDir: URIExecution): Option[ScanResult] = {

    if (!lockDir.isExisting) return None

    val files = lockDir.input { in =>
      in.children
    }

    val lockedFiles: Seq[URIExecution] = files.filter { file =>
      file.absolutePathStr.split('.').lastOption.contains(LockLike.LOCKED)
    }

    val result = _scanForUnlocking(lockedFiles)

    result
  }
}

object LockExpired {}
