package com.tribbloids.spookystuff.utils.io.lock

import com.tribbloids.spookystuff.utils.BypassingRule
import com.tribbloids.spookystuff.utils.io.{URIExecution, URIResolver}
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

case class Lock(
    source: URIExecution,
    expired: LockExpired = URIResolver.default.expired, // TODO: use it!
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM()
) extends LockLike
    with LocalCleanable {

  import Lock._

  @volatile var acquiredTimestamp: Long = -1

  protected def acquire(): URIExecution = {

    try {
      source.moveTo(Moved.locked.absolutePathStr)
    } catch {
      case ee @ (_: FileNotFoundException | _: NoSuchFileException) =>
        val canBeUnlocked = expired.scanForUnlocking(Moved.dir)

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

  protected def release(): Unit = {

    logRelease(Moved.locked)

    if (source.isExisting) {
      source.moveTo(PathStrs.old)
    }

    Moved.locked.moveTo(source.absolutePathStr)
  }

  protected def duringOnce[T](fn: URIExecution => T): T = {
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

  final def during[T](fn: URIExecution => T): T = source.synchronized {
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

//    Lock.acquired += execution -> System.currentTimeMillis()

    this.logPrefixed(s"=== ACQUIRED!: ${execution.absolutePathStr}")
  }

  def logRelease(execution: URIExecution): Unit = {
//    Lock.acquired -= execution

    this.logPrefixed(s"=== RELEASED! ${execution.absolutePathStr}")
  }
}

object Lock {

//  val acquired: CachingUtils.ConcurrentCache[URIExecution, Long] = CachingUtils.ConcurrentCache()

  trait CanReattempt extends Exception

}
