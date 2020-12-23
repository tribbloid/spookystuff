package com.tribbloids.spookystuff.utils.io.lock

import com.tribbloids.spookystuff.utils.io.{URIExecution, URIResolver}
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{BypassingRule, CachingUtils, CommonUtils}

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException
import java.util.UUID

/**
  * Deprecated: Doesn't work for HDFSResolver (and future resolvers for other file systems)!
  * use [[Snapshot]] instead
  */
case class Lock(
    source: URIExecution,
    expired: LockExpired = URIResolver.default.expired, // TODO: use it!
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM()
) extends LocalCleanable {

  import Lock._

  @volatile var acquiredTimestamp: Long = -1

  val resolver: URIResolver = source.outer
  def absolutePathStr: String = source.absolutePathStr

  val id: UUID = UUID.randomUUID()

  private case object Moved {

    case object PathStrs {

      lazy val dir: String = source.absolutePathStr + LOCK

      lazy val locked: String = CommonUtils.\\\(dir, id + LOCKED)

      lazy val old: String = CommonUtils.\\\(dir, id + OLD)
    }

    lazy val locked: resolver.Execution = resolver.Execution(PathStrs.locked)

//    lazy val old: resolver.Execution = resolver.Execution(PathStrs.old)
  }

  def acquire(): URIExecution = {

    try {
      source.moveTo(Moved.locked.absolutePathStr)
    } catch {
      case ee @ (_: FileNotFoundException | _: NoSuchFileException) =>
        val canBeUnlocked = expired.scanForUnlocking(source)

        canBeUnlocked match {
          case Some(v) =>
            v.exe.moveTo(Moved.locked.absolutePathStr)
          case None =>
            throw ee
        }
    }

    logAcquire(source)
    Moved.locked
  }

  def release(): Unit = {

    logRelease(Moved.locked)

    if (source.isExisting) {
      source.moveTo(Moved.PathStrs.old)
    }

    Moved.locked.moveTo(source.absolutePathStr)
  }

  def duringOnce[T](fn: URIExecution => T): T = {
    val acquired = acquire()
    try {
      fn(acquired)
    } catch {
      case e: CanReattempt =>
        throw e
      case e @ _ =>
        throw BypassingRule.NoRetry(e)
    } finally {

      release()
    }
  }

  final def during[T](fn: URIExecution => T): T = {
    resolver.retry {
      duringOnce(fn)
    }
  }

  /**e
    * unlock on cleanup
    */
  override protected def cleanImpl(): Unit = {

    if (Moved.locked.isExisting) release()
  }

  def logAcquire(execution: URIExecution): Unit = {

    Lock.acquired += execution -> System.currentTimeMillis()

    this.logPrefixed(s"=== ACQUIRED!: ${execution.absolutePathStr}")
  }

  def logRelease(execution: URIExecution): Unit = {
    Lock.acquired -= execution

    this.logPrefixed(s"=== RELEASED! ${execution.absolutePathStr}")
  }
}

object Lock {

  val acquired: CachingUtils.ConcurrentCache[URIExecution, Long] = CachingUtils.ConcurrentCache()

  final val LOCK: String = ".lock"

  final val LOCKED: String = ".locked"

  final val OLD: String = ".old"

  trait CanReattempt extends Exception

}
