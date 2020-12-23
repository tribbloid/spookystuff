package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{BypassingRule, CachingUtils, CommonUtils, TreeThrowable}

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

//  def getOriginal: resolver.Execution = resolver.Execution(absolutePathStr)
//  lazy val original: resolver.Execution = getOriginal

  val id: UUID = UUID.randomUUID()

  private case object Moved {

    private case object PathStrs {

      lazy val dir: String = source.absolutePathStr + LOCK

      lazy val file: String = CommonUtils.\\\(dir, id + LOCKED)
//      lazy val file: String = CommonUtils.\\\(dir, LOCKED)
    }

    lazy val target: resolver.Execution = resolver.Execution(PathStrs.file)
  }

  def acquire(): URIExecution = {

    source.moveTo(Moved.target.absolutePathStr)
    logAcquire(source)
    Moved.target
  }

  def release(): Unit = {

    logRelease(Moved.target)
    Moved.target.moveTo(source.absolutePathStr)
    //      moved.clean()
  }

  def duringOnce[T](fn: URIExecution => T): T = {
    val acquired = acquire()
    try {
      fn(acquired)
    } catch {
      case e: LockError => //TODO: remove, pointless
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

//    ByLockFile.release()
    release()
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

  class LockError(
      override val simpleMsg: String = "",
      val cause: Throwable = null
  ) extends Exception
      with TreeThrowable {

    override def getCause: Throwable = cause
  }

}
