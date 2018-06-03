package com.tribbloids.spookystuff.utils.io

import java.nio.file.FileAlreadyExistsException
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.utils.Retry
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.duration.Duration

class Lock(
            execution: URIResolver#Execution,
            lockExpireAfter: Duration = 24 -> TimeUnit.HOURS
          ) extends LocalCleanable {

  def retry: Retry = execution.outer.retry

  final def suffix: String = ".locked"
  lazy val lockPathStr = execution.absolutePathStr + suffix
  lazy val lockExecution = execution.outer.Execution(lockPathStr)

  //  @volatile var _acquired = false

  def acquire() = assertUnlocked(true)

  def assertUnlocked(acquire: Boolean = false): Unit = {
    retry{
      assertUnlockedOnce(acquire)
    }
  }

  @tailrec
  final def assertUnlockedOnce(
                            acquire: Boolean = false
                          ): Unit = {
    var lockExpired = false

    def processLocked(out: Resource[_], e: FileAlreadyExistsException) = {
      val lockedTime = out.getLastModified
      val lockedDuration = System.currentTimeMillis() - lockedTime

      def errorInfo =
        s"Lock '$lockPathStr' is acquired by another executor or thread for $lockedDuration milliseconds"

      if (lockedDuration >= this.lockExpireAfter.toMillis) {
        LoggerFactory.getLogger(this.getClass).error(errorInfo + ", lock has expired")
        lockExpired = true
      }
      else {
        throw new AssertionError(errorInfo, e)
      }
    }

    if (acquire) {
      lockExecution.output(overwrite = false){
        out =>
          try {
            out.stream
          }
          catch {
            case e: FileAlreadyExistsException =>
              processLocked(out, e)
          }
      }
    }
    else {
      lockExecution.input {
        in =>
          if (in.isAlreadyExisting) processLocked(in, null)
      }
    }

    if(lockExpired) {
      release()
      assertUnlockedOnce(acquire)
    }
  }

  //  override def _lifespan: Lifespan = Lifespan.Auto() //TODO: enable?

  def release() = {
    lockExecution.remove(false)
  }

  /**
    * unlock on cleanup
    */
  override protected def cleanImpl(): Unit = {

    release()
  }
}
