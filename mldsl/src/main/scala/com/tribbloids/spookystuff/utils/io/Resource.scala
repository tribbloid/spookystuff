package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}

import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.spark.ml.dsl.utils.data.EAV

import scala.language.implicitConversions

abstract class Resource[T] extends LocalCleanable {

  import Resource._

  @volatile protected var existingStream: T = _
  protected def _stream: T
  final lazy val stream: T = Option(existingStream).getOrElse {
    existingStream = _stream
    existingStream
  }

  def getURI: String
  def getPath: String = getURI

  def getName: String

  def getType: String = UNKNOWN
  final lazy val isDirectory: Boolean = getType == DIR

  def getContentType: String
  def getLenth: Long
  def getStatusCode: Option[Int] = None

  def getLastModified: Long

  def isAlreadyExisting: Boolean

  protected def _md: ResourceMetadata

  lazy val rootMetadata: ResourceMetadata = {
    val reflective = Resource.resourceParser(this)
    new ResourceMetadata(reflective ++: _md)
  }

  def children: Seq[ResourceMetadata] = Nil

  lazy val allMetadata: ResourceMetadata = {

    val grouped = children.groupBy(_.asMap("Type").toString)
    val childMaps: Map[String, Seq[Map[String, Any]]] = grouped.mapValues {
      _.map { md =>
        md.asMap
      }
    }

    val result = new ResourceMetadata(rootMetadata :++ childMaps)
    result
  }

  //  override def cleanImpl(): Unit = {}
}

abstract class InputResource extends Resource[InputStream] {

  override def cleanImpl(): Unit =
    Option(existingStream)
      .foreach(_.close)
}
abstract class OutputResource extends Resource[OutputStream] {

  override def cleanImpl(): Unit =
    Option(existingStream)
      .foreach { v =>
        v.flush()
        v.close()
      }
}

///**
//  * lazy execution disabled
//  * @param value
//  * @param metadata
//  * @tparam T
//  */
//case class Resource_[+T](
//                          value: T,
//                          metadata: ResourceMD
//                        ) extends Resource[T]

object Resource extends {

  //  implicit def unbox[T](obj: Resource[T]): T = obj.body

  val resourceParser = EAV.Impl.ReflectionParser[Resource[_]]()

  final val DIR = "directory"
  final val DIR_MIME = "inode/directory; charset=UTF-8"
  final val UNKNOWN = "unknown"

//  implicit def view(self: ResourceMD) = new ResourceMDView(self)
}
