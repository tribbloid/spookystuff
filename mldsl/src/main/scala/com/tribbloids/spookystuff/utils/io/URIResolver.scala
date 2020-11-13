package com.tribbloids.spookystuff.utils.io

import java.io._
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import com.tribbloids.spookystuff.utils.{CommonUtils, Retry, RetryExponentialBackoff}

import scala.concurrent.duration.Duration

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node.
 *
 * also, can ONLY resolve ABSOLUTE path! since its instances cannot be guaranteed to be in the same JVM,
 * this is the only way to guarantee that files are not affected by their respective working directory.
 */
abstract class URIResolver extends Serializable {

  def Execution(pathStr: String): this.Execution

  def retry: Retry = RetryExponentialBackoff(8, 16000)
  def lockExpireAfter: Duration = URIResolver.defaultLockExpireAfter

  lazy val unlockForInput: Boolean = false

  final def input[T](pathStr: String)(f: InputResource => T): T = {
    val exe = Execution(pathStr)
    exe.input(f)
  }

  final def output[T](pathStr: String, overwrite: Boolean)(f: OutputResource => T): T = {
    val exe = Execution(pathStr)
    exe.output(overwrite)(f)
  }

  final def toAbsolute(pathStr: String): String = Execution(pathStr).absolutePathStr

  final def isAbsolute(pathStr: String): Boolean = {
    toAbsolute(pathStr) == pathStr
  }

  def ensureAbsolute(file: File): Unit = {
    assert(file.isAbsolute, s"BAD DESIGN: ${file.getPath} is not an absolute path")
  }

  def resourceOrAbsolute(pathStr: String): String = {
    val resourcePath = CommonUtils.getCPResource(pathStr.stripPrefix("/")).map(_.getPath).getOrElse(pathStr)

    val result = this.toAbsolute(resourcePath)
    result
  }

  @Deprecated // TODO: broken, need better abstraction
  def lockAccessDuring[T](pathStr: String)(f: String => T): T = {
    val lock = new Lock(Execution(pathStr))
    val path = lock.acquire()
    try {
      f(path)
    } finally {
      lock.clean()
    }
  }

  def isAlreadyExisting(pathStr: String)(
      condition: InputResource => Boolean = { v =>
        v.getLenth > 0 //empty files are usually useless
      }
  ): Boolean = {
    val result = this.input(pathStr) { v =>
      v.isAlreadyExisting && condition(v)
    }
    result
  }

  /**
    * ensure sequential access
    *
    */
  //  def lockAccessDuring[T](pathStr: String)(f: String => T): T = {f(pathStr)}

  trait Execution extends NOTSerializable {

    def outer: URIResolver = URIResolver.this

    def absolutePathStr: String

    // read: may execute lazily
    def input[T](f: InputResource => T): T

    // remove & write: execute immediately! write an empty file even if stream is not used
    protected[io] def _remove(mustExist: Boolean = true): Unit
    final def remove(mustExist: Boolean = true): Unit = {
      _remove(mustExist)
      retry {
        input { in =>
          assert(!in.isAlreadyExisting, s"$absolutePathStr cannot be deleted")
        }
      }
    }

    def output[T](overwrite: Boolean)(f: OutputResource => T): T

    //    final def peek(): Unit] = {
    //      read({_ =>})
    //    }
    //
    //    final def touch(overwrite: Boolean = false): Resource[Unit] = {
    //      write(overwrite, { _ =>})
    //
    //      //          retry { //TODO: necessary?
    //      //            assert(
    //      //              fs.exists(lockPath),
    //      //              s"Lock '$lockPath' cannot be persisted")
    //      //          }
    //    }
  }
}

object URIResolver {

  val defaultLockExpireAfter: Duration = 24 -> TimeUnit.HOURS
}
