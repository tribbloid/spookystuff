package com.tribbloids.spookystuff.utils.io.lock

import com.tribbloids.spookystuff.utils.CachingUtils
import com.tribbloids.spookystuff.utils.io.lock.Lock.InMemoryLock
import com.tribbloids.spookystuff.utils.io.{URIExecution, URIResolver}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

case class Lock(
    exe: URIExecution,
    expired: LockExpired = URIResolver.default.expired,
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM().forShipping
) extends LockLike
    with LocalCleanable {

  @volatile var acquiredTimestamp: Long = -1

  @transient lazy val inMemory: InMemoryLock = {
    val result = Lock.inMemoryLocks.getOrElseUpdate(exe.outer.getClass -> exe.absolutePathStr, InMemoryLock())
    result
  }

  protected def acquire(): URIExecution = {

    try {
      exe.moveTo(Moved.locked.absolutePathStr)
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

    logAcquire(exe)
    Moved.locked
  }

  protected def release(): Unit = {

    logRelease(Moved.locked)

    if (exe.isExisting) {
      exe.moveTo(PathStrs.old)
    }

    Moved.locked.moveTo(exe.absolutePathStr)
  }

  protected def duringOnce[T](fn: URIExecution => T): T = {
    val acquired = acquire()
    try {
      fn(acquired)
    } finally {

      release()
    }
  }

  final def during[T](fn: URIExecution => T): T = inMemory.synchronized {
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

  case class InMemoryLock() {
//    lazy val id: Long = Random.nextLong()
  }

  lazy val inMemoryLocks: CachingUtils.ConcurrentCache[(Class[_], String), InMemoryLock] =
    CachingUtils.ConcurrentCache()
}
