package com.tribbloids.spookystuff.utils.io

import java.io._
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.utils.{CommonUtils, NOTSerializable, Retry, RetryExponentialBackoff}

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node.
 *
 * also, can ONLY resolve ABSOLUTE path! since its instances cannot be guaranteed to be in the same JVM,
 * this is the only way to guarantee that files are not affected by their respective working directory.
 */
abstract class URIResolver extends Serializable {

  def Execution(pathStr: String): this.Execution
  protected[io] def retry: Retry = RetryExponentialBackoff(8, 16000)
  protected[io] def lockExpireAfter: Duration = URIResolver.defaultLockExpireAfter

  lazy val unlockForInput: Boolean = false

  final def input[T](pathStr: String, unlockFirst: Boolean = unlockForInput)(f: InputResource => T): T = {
    val exe = Execution(pathStr)
    if (unlockFirst) {
      val lock = new Lock(Execution(pathStr))
      lock.assertUnlocked()
    }
    exe.input(f)
  }

  final def output[T](pathStr: String, overwrite: Boolean)(f: OutputResource => T): T = Execution(pathStr)
    .output(overwrite)(f)

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

  def lockAccessDuring[T](pathStr: String)(f: String => T) = {
    val lock = new Lock(Execution(pathStr))
    val path = lock.acquire()
    try {
      f(path)
    }
    finally {
      lock.close()
    }
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
    def remove(mustExist: Boolean = true): Unit
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