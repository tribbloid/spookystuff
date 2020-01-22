package com.tribbloids.spookystuff.utils.io

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

import scala.concurrent.duration.Duration

case class Obsolescence(
    ignoreAfter: Duration,
    deleteAfter: Duration
) {

  case class Verdict(elapsedMillis: Long) {

    lazy val shouldBeIgnored: Boolean = elapsedMillis >= ignoreAfter.toMillis
    lazy val shouldBeDeleted: Boolean = elapsedMillis >= deleteAfter.toMillis
  }

  def checkSession(v: URISession): Option[Verdict] = {

    v.input { in =>
      if (v.absolutePathStr.split('/').lastOption.contains(Snapshot.MASTER))
        return None

      try {
        val lastModified = in.getLastModified

        val elapsed = System.currentTimeMillis() - lastModified
        val result = Verdict(elapsed)
        Some(result)
      } catch {
        case e: FileNotFoundException => None
        case e: NoSuchFileException   => None
      }
    }
  }

  def filter(vs: Seq[URISession]): Seq[URISession] = {

    vs.flatMap { v =>
      val verdictOpt = checkSession(v)

      verdictOpt.flatMap { verdict =>
        if (verdict.shouldBeDeleted) {
          v.delete(false)
          None
        } else if (verdict.shouldBeIgnored) {
          None
        } else {
          Some(v)
        }
      }
    }
  }
}

object Obsolescence {}
