package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.commons.io.output.NullOutputStream
import org.apache.spark.ml.dsl.utils.LazyVar
import org.apache.spark.ml.dsl.utils.data.{EAV, EAVCore}

import scala.language.implicitConversions
import scala.util.Try

abstract class Resource extends LocalCleanable {

  def mode: WriteMode

  import Resource._

  protected def _newIStream: InputStream
  protected def newIStream: InputStream = _newIStream

  protected def _newOStream: OutputStream
  protected def newOStream: OutputStream = mode match {
    case WriteMode.ReadOnly => throw new UnsupportedOperationException("cannot write if mode is ReadOnly")
    case WriteMode.Ignore   => NullOutputStream.NULL_OUTPUT_STREAM
    case _                  => _newOStream
  }

  case class _IO[T](streamFactory: () => T) extends IO {

    lazy val _stream: LazyVar[T] = LazyVar {
      streamFactory()
    }

    def stream: T = _stream.value

    final val outer: Resource.this.type = Resource.this
  }

  object InputView extends _IO(() => newIStream) {}
  type InputView = InputView.type

  object OutputView extends _IO(() => newOStream) {}
  type OutputView = OutputView.type

  def getURI: String

  def getName: String

  def getType: String
  final lazy val isDirectory: Boolean = getType == DIR

  def getContentType: String
  def getLength: Long
  def getStatusCode: Option[Int] = None

  def getLastModified: Long

  def isExisting: Boolean = Try(getType).isSuccess

  protected def _metadata: ResourceMetadata

  def children: Seq[URIExecution] = Nil

  case object metadata {

    final lazy val root: ResourceMetadata = {
      val reflective = Resource.resourceParser(Resource.this)
      new ResourceMetadata(reflective ++: _metadata)
    }

    final lazy val all: ResourceMetadata = {

      val grouped = children
        .map(exe => exe.input(in => in.metadata.root))
        .groupBy(_.asMap("Type").toString)

      val childMaps: Map[String, Seq[Map[String, Any]]] = grouped.mapValues {
        _.map { md =>
          md.asMap
        }
      }

      val result = new ResourceMetadata(root :++ childMaps)
      result
    }

  }

  override def cleanImpl(): Unit = {
    InputView._stream.peek.foreach { v =>
      v.close()
    }
    OutputView._stream.peek.foreach { v =>
      v.flush()
      v.close()
    }
  }
}

object Resource extends {

  trait IO {

    val outer: Resource
  }

  object IO {

    implicit def asResource[R <: Resource, T](io: R#_IO[T]): R = io.outer
  }

  val resourceParser: EAVCore.ReflectionParser[Resource] = EAV.Impl.ReflectionParser[Resource]()

  final val DIR = "directory"
  final val FILE = "file"
  final val SYMLINK = "symlink"
  final val UNKNOWN = "unknown"

  def mimeIsDir(mime: String): Boolean = {

    val effective = mime.split('/').last.split(';').head.trim.toLowerCase

    effective == DIR || effective == "dir"
  }

  final val DIR_MIME_OUT = "inode/directory; charset=UTF-8"
}
