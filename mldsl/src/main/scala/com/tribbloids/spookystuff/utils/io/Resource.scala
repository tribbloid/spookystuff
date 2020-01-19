package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}

import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.spark.ml.dsl.utils.LazyVar
import org.apache.spark.ml.dsl.utils.data.{EAV, EAVCore}

abstract class Resource[T] extends LocalCleanable {

  import Resource._

  protected def createStream: T

  final protected lazy val _stream: LazyVar[T] = LazyVar {
    createStream
  }
  def stream: T = _stream.value

  def getURI: String
  def getPath: String = getURI

  def getName: String

  def getType: String = UNKNOWN
  final lazy val isDirectory: Boolean = getType == DIR

  def getContentType: String
  def getLength: Long
  def getStatusCode: Option[Int] = None

  def getLastModified: Long

  def isExisting: Boolean

  protected def _metadata: ResourceMetadata

  def children: Seq[URIExecution] = Nil

  case object metadata {

    final lazy val root: ResourceMetadata = {
      val reflective = Resource.resourceParser(Resource.this)
      new ResourceMetadata(reflective ++: _metadata)
    }

    final lazy val all: ResourceMetadata = {

      val grouped = children
        .map(session => session.input(in => in.metadata.root))
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
  //  override def cleanImpl(): Unit = {}
}

abstract class InputResource extends Resource[InputStream] {

  override def cleanImpl(): Unit = _stream.peek.foreach(_.close())
}

abstract class OutputResource extends Resource[OutputStream] {

  override def cleanImpl(): Unit =
    _stream.peek.foreach { v =>
      v.flush()
      v.close()
    }
}

object Resource extends {

  val resourceParser: EAVCore.ReflectionParser[Resource[_]] = EAV.Impl.ReflectionParser[Resource[_]]()

  final val DIR = "directory"
  final val FILE = "file"

  final val DIR_MIME = "inode/directory; charset=UTF-8"
  final val UNKNOWN = "unknown"

}
