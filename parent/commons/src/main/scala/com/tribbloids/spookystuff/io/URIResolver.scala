package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.util.Retry
import com.tribbloids.spookystuff.io.lock.{Lock, LockExpired}
import org.apache.commons.io.IOUtils

import java.io.*
import scala.concurrent.duration.DurationInt
import scala.util.Random

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node.
 *
 * also, can ONLY resolve ABSOLUTE path! since its instances cannot be guaranteed to be in the same JVM,
 * this is the only way to guarantee that files are not affected by their respective working directory.
 */
abstract class URIResolver extends Serializable {

  /**
    * entry for I/O operations for a given path
    *
    * all implementations must be stateless, such that a single execution can be used for multiple I/O operations,
    * potentially in different threads
    */
  trait Execution {

    type _Resource <: Resource
    def _Resource: WriteMode => _Resource

    def outer: URIResolver = URIResolver.this

    def absolutePathStr: String

    protected def _delete(mustExist: Boolean = true): Unit

    final def delete(mustExist: Boolean = true): Unit = {
      _delete(mustExist)
      //      retry {
      //        input { in =>
      //          assert(!in.isAlreadyExisting, s"$absolutePathStr cannot be deleted")
      //        }
      //      }
    }

    def moveTo(target: String, force: Boolean = false): Unit

    final def copyTo(targetExe: URIResolver#_Execution, mode: WriteMode): Unit = {

      this.input { in =>
        targetExe.output(mode) { out =>
          IOUtils.copy(in.stream, out.stream)
        }
      }
    }

    final def copyTo(target: String, mode: WriteMode): Unit = {

      val tgtExe = outer.execute(target)

      copyTo(tgtExe, mode)
    }

    final def createNew(): Unit = create_simple()

    def zeroByte: Array[Byte] = Array.empty[Byte]

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

    final def isExisting: Boolean = {
      doIO()(_.isExisting)
    }

    final def isNonEmpty: Boolean = satisfy { v =>
      v.getLength > 0
    }

    final def satisfy(
        condition: _Resource => Boolean
    ): Boolean = {

      val result = doIO() { v =>
        v.isExisting && condition(v)
      }
      result
    }

    final def input[T](fn: _Resource#InputView => T): T = doIO() { v =>
      v.tryRequireExisting.get

      fn(v.InputView)
    }

    protected def doIO[T](mode: WriteMode = WriteMode.ReadOnly)(fn: _Resource => T): T = {

      val resource = _Resource(mode)

      try {
        fn(resource)
      } finally {
        resource.clean()
      }
    }

//     write an empty file even if stream is not used
    final def output[T](mode: WriteMode = WriteMode.CreateOnly)(fn: _Resource#OutputView => T): T = doIO(mode) { v =>
      fn(v.OutputView)
    }

//    override protected def cleanImpl(): Unit = {}
  }

  type _Execution <: Execution
  def _Execution: String => _Execution

  final type _Resource = _Execution#_Resource

  final def execute(pathStr: String): _Execution = {
    _Execution(pathStr)
  }

  def retry: Retry = URIResolver.default.retry
  def lockExpire: LockExpired = URIResolver.default.lockExpired

  final def input[T](pathStr: String)(f: _Resource#InputView => T): T = {
    val exe = execute(pathStr)
    exe.input(f)
  }

  final def output[T](pathStr: String, mode: WriteMode)(f: _Resource#OutputView => T): T = {
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

//  def resourceOrAbsolute(pathStr: String): String = {
//    val resourcePath =
//      ClasspathResolver.Execution(pathStr.stripPrefix("/")).resourceOpt.map(_.getPath).getOrElse(pathStr)
//
//    val result = this.toAbsolute(resourcePath)
//    result
//  }

  /**
    * ensure sequential access, doesn't work on non-existing path
    */
  def lock[T](pathStr: String)(fn: URIExecution => T): T = {
    val exe = execute(pathStr)

    val lock = new Lock(exe, lockExpire)

    lock.during(fn)
  }

  def unsupported(op: String): Nothing = {
    throw new UnsupportedOperationException(
      s"Implementation doesn't support ${this.getClass.getSimpleName}.$op operation"
    )
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

    val lockExpired: LockExpired = LockExpired(
      unlockAfter = 30.seconds,
      deleteAfter = 1.hours
    )
  }

  final val TOUCH = ".touch"
}
