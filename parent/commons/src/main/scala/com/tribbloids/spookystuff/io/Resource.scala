package com.tribbloids.spookystuff.io

import com.tribbloids.spookystuff.commons.lifespan.LocalCleanable
import org.apache.commons.io.output.NullOutputStream
import org.apache.spark.ml.dsl.utils.LazyVar

import java.io.{IOException, InputStream, OutputStream}
import scala.language.implicitConversions
import scala.util.Try

abstract class Resource extends LocalCleanable {

  def mode: WriteMode

  import Resource.*

  protected def _newIStream: InputStream
  protected def newIStream: InputStream = {
    _newIStream
  }

  protected def _newOStream: OutputStream
  protected def newOStream: OutputStream = mode match {
    case WriteMode.ReadOnly => throw new UnsupportedOperationException("cannot write if mode is ReadOnly")
    case WriteMode.Ignore   => NullOutputStream.NULL_OUTPUT_STREAM
    case _                  => _newOStream
  }

  case class _IOView[T](streamFactory: () => T) extends IOView {

    lazy val _stream: LazyVar[T] = LazyVar {
      streamFactory()
    }

    def stream: T = _stream.value

    final val outer: Resource.this.type = Resource.this
  }

  object InputView extends _IOView(() => newIStream) {}
  type InputView = InputView.type

  object OutputView extends _IOView(() => newOStream) {}
  type OutputView = OutputView.type

  protected def _outer: URIExecution

  lazy val getURI: String = _outer.absolutePath

  def getName: String

  def getType: String
  final lazy val isDirectory: Boolean = getType == DIR

  protected def _requireExisting(): Unit = {
    getType
  }

  final def tryRequireExisting: Try[Unit] = Try {
    try {
      _requireExisting()
    } catch {
      case e: Exception =>
        val bothPaths = Seq(_outer.absolutePath, getURI).distinct.mkString(" ~> ")
        throw new IOException(s"Resource ${bothPaths} does not exist", e)
    }
  }

  final def isExisting: Boolean = Try(_requireExisting()).isSuccess

  def getContentType: String
  def getLength: Long
  def getStatusCode: Option[Int] = None

  def getLastModified: Long

  protected def _metadata: ResourceMetadata

  def children: Seq[URIExecution] = Nil

  case object metadata {

    // TODO: this should not be necessary

    final lazy val root: ResourceMetadata = {

      val reflective: ResourceMetadata = Resource.resourceParser.apply(Resource.this)
      val result = reflective.++:(_metadata)
      result
    }

    final lazy val all: ResourceMetadata = {

      val grouped: Map[String, Seq[ResourceMetadata]] = children
        .map(exe => exe.input(in => in.metadata.root))
        .groupBy(_.lookup("Type").toString)

      val childMaps = grouped.view.mapValues { vs =>
        val sorted = vs.sortBy(_.sortEvidence)

        sorted
          .map { md =>
            md.internal
          }
      }.toMap

      val result = root :++ ResourceMetadata.^(childMaps)
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

object Resource {

  trait IOView {

    val outer: Resource
  }

  object IOView {

    implicit def asResource[R <: Resource, T](io: R#_IOView[T]): R = io.outer
  }

  val resourceParser: ResourceMetadata.ReflectionParser[Resource] = ResourceMetadata.ReflectionParser[Resource]()

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
