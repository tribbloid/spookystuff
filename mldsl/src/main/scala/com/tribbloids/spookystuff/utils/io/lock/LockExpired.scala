package com.tribbloids.spookystuff.utils.io.lock

import com.tribbloids.spookystuff.utils.io.URIExecution

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException
import scala.concurrent.duration.Duration

case class LockExpired(
    unlockAfter: Duration,
    deleteAfter: Duration
) {

  case class Finding(exe: URIExecution) {

    val lastModified: Long = exe.input { in =>
      in.getLastModified

    }

    val elapsedMillis: Long = System.currentTimeMillis() - lastModified

    lazy val canBeUnlocked: Boolean = elapsedMillis >= unlockAfter.toMillis
    lazy val canBeDeleted: Boolean = canBeUnlocked && elapsedMillis >= deleteAfter.toMillis
  }

  protected def _scanForUnlocking(vs: Seq[URIExecution]): Option[Finding] = {

    val findings = vs.flatMap { v =>
      try {
        Some(Finding(v))
      } catch {
        case _: FileNotFoundException => None
        case _: NoSuchFileException   => None
      }
    }

    val canBeUnlocked = findings.filter(_.canBeUnlocked)

    val latest = canBeUnlocked.sortBy(_.lastModified).lastOption

    val canBeDeleted = canBeUnlocked.filter(v => latest.contains(v)).filter(_.canBeDeleted)

    canBeDeleted.foreach { v =>
      v.exe.delete(false)
    }

    latest
  }

  def scanForUnlocking(file: URIExecution): Option[Finding] = {

    try {
      val lockDir = file.outer.Execution(file.absolutePathStr + Lock.LOCK)

      val files = lockDir.input { in =>
        in.children
      }

      val lockedFiles: Seq[URIExecution] = files.filter { file =>
        file.absolutePathStr.split('.').lastOption.contains(Lock.LOCKED)
      }

      _scanForUnlocking(lockedFiles)
    }
  }
}

object LockExpired {}
