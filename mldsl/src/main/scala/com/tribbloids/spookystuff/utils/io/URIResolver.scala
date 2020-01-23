package com.tribbloids.spookystuff.utils.io

import java.io._
import java.util.concurrent.TimeUnit

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CommonUtils, Retry}
import org.apache.commons.io.IOUtils

import scala.util.Random

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node.
 *
 * also, can ONLY resolve ABSOLUTE path! since its instances cannot be guaranteed to be in the same JVM,
 * this is the only way to guarantee that files are not affected by their respective working directory.
 */
abstract class URIResolver extends Serializable {

  def newSession(pathStr: String): this.URISession

  def retry: Retry = URIResolver.default.retry

//  lazy val unlockForInput: Boolean = false

  final def input[T](pathStr: String)(f: InputResource => T): T = {
    val exe = newSession(pathStr)
//    if (unlockFirst) {
//      val lock = new Lock(newSession(pathStr))
//      lock.assertUnlocked()
//    }
    exe.input(f)
  }

  final def output[T](pathStr: String, mode: WriteMode)(f: OutputResource => T): T = {
    val exe = newSession(pathStr)
    exe.output(mode)(f)
  }

  final def toAbsolute(pathStr: String): String = newSession(pathStr).absolutePathStr

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

  final def snapshot(
      pathStr: String,
      lockExpireAfter: Obsolescence = URIResolver.default.expire,
      lifespan: Lifespan = Lifespan.JVM()
  ): Snapshot = {
    Snapshot(newSession(pathStr), lockExpireAfter, lifespan)
  }

  final def lock[T](pathStr: String): Lock = {
    Lock(this, pathStr)
  }

  def isAlreadyExisting(pathStr: String)(
      condition: InputResource => Boolean = { v =>
        v.getLength > 0 //empty files are usually useless
      }
  ): Boolean = {
    val result = this.input(pathStr) { v =>
      v.isExisting && condition(v)
    }
    result
  }

  /**
    * ensure sequential access
    *
    */
  //  def lockAccessDuring[T](pathStr: String)(f: String => T): T = {f(pathStr)}

  trait URISession extends LocalCleanable {

    def outer: URIResolver = URIResolver.this

    def absolutePathStr: String

    // remove & write: execute immediately! write an empty file even if stream is not used
    protected[io] def _delete(mustExist: Boolean = true): Unit
    final def delete(mustExist: Boolean = true): Unit = {
      _delete(mustExist)
//      retry {
//        input { in =>
//          assert(!in.isAlreadyExisting, s"$absolutePathStr cannot be deleted")
//        }
//      }
    }

    def moveTo(target: String): Unit

    //removed, no need if creating new file is always recursive
//    def mkDirs(): Unit

    final def copyTo(target: String, mode: WriteMode): Unit = {

      val tgtSession = outer.newSession(target)

      this.input { in =>
        tgtSession.output(mode) { out =>
          IOUtils.copy(in.stream, out.stream)
        }
      }
    }

    final def createNew(): Unit = create_simple()

    private def create_simple(): Unit = {

      val mark = Seq(Random.nextInt().byteValue())

      try {
        this.output(WriteMode.CreateOnly) { out =>
          out.stream.write(mark.toArray)
        }

        this.input { in =>
          val v = IOUtils.toByteArray(in.stream)
          require(v.toSeq == mark)
        }
      } catch {
        case e: Throwable =>
          this.delete(false)
          throw e
      }

    }

    private def create_complex(): Unit = {

      val touchSession: URIResolver#URISession = {

        val touchPathStr = this.absolutePathStr + URIResolver.TOUCH

        outer.newSession(touchPathStr)
      }

      // this convoluted way of creating file is to ensure that atomic contract can be engaged
      // TODO: not working in any FS! why?

      val mark = Seq(Random.nextInt().byteValue())

      touchSession.output(WriteMode.CreateOnly) { out =>
        out.stream.write(mark.toArray)
      }

      try {
        touchSession.moveTo(this.absolutePathStr)

        this.input { in =>
          val v = IOUtils.toByteArray(in.stream)
          require(v.toSeq == mark)
        }

        this.output(WriteMode.Overwrite) { out =>
          out.stream
        }

      } catch {
        case e: Throwable =>
          touchSession.delete(false)
          throw e
      } finally {

        touchSession.clean()
      }
    }

    final def isExisting: Boolean = {
      input(_.isExisting)
    }

    // read: may execute lazily
    def input[T](fn: InputResource => T): T

    def output[T](mode: WriteMode)(fn: OutputResource => T): T

    override protected def cleanImpl(): Unit = {}
  }

}

object URIResolver {

  object default {

    //  val defaultRetry: Retry = Retry.ExponentialBackoff(10, 32000, 1.5, silent = false)
    val retry: Retry = Retry(
      n = 16,
      intervalFactory = { n =>
        (10000.doubleValue() / Math.pow(1.2, n - 2)).asInstanceOf[Long] + Random.nextInt(1000).longValue()
      },
      silent = true
    )

    val expire: Obsolescence = Obsolescence(
      ignoreAfter = 30 -> TimeUnit.SECONDS,
      deleteAfter = 1 -> TimeUnit.HOURS
    )
  }

  final val TOUCH = ".touch"
}
