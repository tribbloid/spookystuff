package com.tribbloids.spookystuff.utils.io

import java.nio.file.FileAlreadyExistsException

import com.tribbloids.spookystuff.utils.Retry
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

class Lock(
            execution: URIResolver#Execution
          ) extends LocalCleanable {

  import Lock._

  def retry: Retry = execution.outer.retry
  def expireAfter = execution.outer.lockExpireAfter

  lazy val lockPathStr = execution.absolutePathStr + SUFFIX
  lazy val lockExecution = execution.outer.Execution(lockPathStr)

  //  @volatile var _acquired = false

  def acquire() = {
    assertUnlocked(true)
    execution.absolutePathStr
  }

  def assertUnlocked(acquire: Boolean = false): Unit = {
    retry{
      assertUnlockedOnce(acquire)
    }
  }

  @tailrec
  protected final def assertUnlockedOnce(
                                          acquire: Boolean = false
                                        ): Unit = {
    var lockExpired = false

    def processLocked(out: Resource[_], e: FileAlreadyExistsException) = {
      val lockedTime = out.getLastModified
      val lockedDuration = System.currentTimeMillis() - lockedTime

      def errorInfo =
        s"Lock '$lockPathStr' is acquired by another executor or thread for $lockedDuration milliseconds"

      if (lockedDuration >= expireAfter.toMillis) {
        LoggerFactory.getLogger(this.getClass).error(errorInfo + " and has expired")
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

object Lock {

  final val SUFFIX: String = ".locked"
}