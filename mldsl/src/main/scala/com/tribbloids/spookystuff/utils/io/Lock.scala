package com.tribbloids.spookystuff.utils.io

import java.nio.file.FileAlreadyExistsException

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

class Lock(
            execution: URIResolver#Execution
          ) extends LocalCleanable {

  import Lock._
  val resolver = execution.outer
  import resolver._

  lazy val lockPathStr = execution.absolutePathStr + SUFFIX
  lazy val lockExecution = Execution(lockPathStr)

  //  @volatile var _acquired = false

  def acquire() = {
    assertUnlocked(true)
    execution.absolutePathStr
  }

  def assertUnlocked(acquire: Boolean = false): Unit = {
    retry{
      //TODO: fail early for non LockError
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

      if (lockedDuration >= lockExpireAfter.toMillis) {
        LoggerFactory.getLogger(this.getClass).error(errorInfo + " and has expired")
        lockExpired = true
      }
      else {
        throw new ResourceLockError(errorInfo, e)
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

  override def _lifespan: Lifespan = Lifespan.Auto() //TODO: enable?

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

  final val SUFFIX: String = ".lock"
}