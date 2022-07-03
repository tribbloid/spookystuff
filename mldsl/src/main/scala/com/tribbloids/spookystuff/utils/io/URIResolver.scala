package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.io.Resource.{InputResource, OutputResource}
import com.tribbloids.spookystuff.utils.io.lock.{Lock, LockExpired}
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import com.tribbloids.spookystuff.utils.{CommonUtils, Retry}
import org.apache.commons.io.IOUtils

import java.io._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.Random

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node.
 *
 * also, can ONLY resolve ABSOLUTE path! since its instances cannot be guaranteed to be in the same JVM,
 * this is the only way to guarantee that files are not affected by their respective working directory.
 */
abstract class URIResolver extends Serializable {

  type Execution <: AbstractExecution

  final def execute(pathStr: String): Execution = {
    newExecution(pathStr)
  }

  def newExecution(pathStr: String): Execution

  def retry: Retry = URIResolver.default.retry
  def lockExpireAfter: Duration = URIResolver.defaultLockExpireAfter

  final def input[T](pathStr: String)(f: InputResource => T): T = {
    val exe = execute(pathStr)
    exe.input(f)
  }

  final def output[T](pathStr: String, mode: WriteMode)(f: OutputResource => T): T = {
    val exe = execute(pathStr)
    exe.output(mode)(f)
  }

  final def toAbsolute(pathStr: String): String = execute(pathStr).absolutePathStr

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

  /**
    * ensure sequential access, doesn't work on non-existing path
    */
  def lock[T](pathStr: String)(fn: URIExecution => T): T = {
    val exe = execute(pathStr)

    val lock = new Lock(exe)

    lock.during(fn)
  }

  def unsupported(op: String): Nothing = {
    throw new UnsupportedOperationException(
      s"Implementation doesn't support ${this.getClass.getSimpleName}.$op operation"
    )
  }

  /**
    * entry for I/O operations for a given path
    *
    * all implementations must be stateless, such that a single execution can be used for multiple I/O operations,
    * potentially in different threads
    */
  trait AbstractExecution extends LocalCleanable {

    def outer: URIResolver = URIResolver.this

    def absolutePathStr: String

    protected[io] def _delete(mustExist: Boolean = true): Unit
    final def delete(mustExist: Boolean = true): Unit = {
      _delete(mustExist)
//      retry {
//        input { in =>
//          assert(!in.isAlreadyExisting, s"$absolutePathStr cannot be deleted")
//        }
//      }
    }

    def moveTo(target: String, force: Boolean = false): Unit

    //removed, no need if creating new file is always recursive
//    def mkDirs(): Unit

    final def copyTo(target: String, mode: WriteMode): Unit = {

      val tgtSession = outer.execute(target)

      this.input { in =>
        tgtSession.output(mode) { out =>
          IOUtils.copy(in.stream, out.stream)
        }
      }
    }

    final def createNew(): Unit = create_simple()

    def zeroByte = Array.empty[Byte]

    private def create_simple(): Unit = {

      try {
        this.output(WriteMode.CreateOnly) { out =>
          out.stream.write(zeroByte)
        }

        this.input { in =>
          val v = IOUtils.toByteArray(in.stream)
          require(v.toSeq == zeroByte.toSeq)
        }
      } catch {
        case e: Exception =>
          this.delete(false)
          throw e
      }

    }

    @Deprecated // use create_simple instead
    private def create_complex(): Unit = { //TODO: remove

      val touchSession: URIExecution = {

        val touchPathStr = this.absolutePathStr + URIResolver.TOUCH

        outer.execute(touchPathStr)
      }

      // this convoluted way of creating file is to ensure that atomic contract can be engaged
      // TODO: not working in any FS! why?

      touchSession.output(WriteMode.CreateOnly) { out =>
        out.stream.write(zeroByte)
      }

      try {
        touchSession.moveTo(this.absolutePathStr)

        this.input { in =>
          val v = IOUtils.toByteArray(in.stream)
          require(v.toSeq == zeroByte.toSeq)
        }

        this.output(WriteMode.Overwrite) { out =>
          out.stream
        }

      } catch {
        case e: Exception =>
          touchSession.delete(false)
          throw e
      } finally {

        touchSession.clean()
      }
    }

    final def isExisting: Boolean = {
      output()(_.isExisting)
    }

    final def isNonEmpty: Boolean = satisfy { v =>
      v.getLength > 0
    }

    final def satisfy(
        condition: InputResource => Boolean
    ): Boolean = {

      val result = input { v =>
        v.isExisting && condition(v)
      }
      result
    }

    // read: may execute lazily
    def input[T](fn: InputResource => T): T

    // write an empty file even if stream is not used
    def output[T](mode: WriteMode = WriteMode.CreateOnly)(fn: OutputResource => T): T

    override protected def cleanImpl(): Unit = {}
  }

}

object URIResolver {

  object default {

    val retry: Retry = Retry(
      n = 16,
      intervalFactory = { n =>
        (10000.doubleValue() / Math.pow(1.2, n - 2)).asInstanceOf[Long] + Random.nextInt(1000).longValue()
      },
//      silent = true
      silent = false
    )

    val expired: LockExpired = LockExpired(
      unlockAfter = 30 -> TimeUnit.SECONDS,
      deleteAfter = 1 -> TimeUnit.HOURS
    )
  }

  final val TOUCH = ".touch"

  val defaultLockExpireAfter: Duration = 24 -> TimeUnit.HOURS
}
