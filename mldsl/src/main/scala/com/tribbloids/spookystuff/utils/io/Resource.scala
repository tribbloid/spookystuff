package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}

import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import org.apache.spark.ml.dsl.utils.metadata.Metadata

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

@deprecated
sealed class ResourceMDView(
                             val self: ResourceMD
                           ) {

  import self._

  object `uri` extends Param[String]
  object `name` extends Param[String]
  object `type` extends Param[String]
  object `content-type` extends Param[String]
  object `length` extends Param[Long]
  object `status-code` extends Param[Int]

  object `isDir` extends Param[Boolean]
}

abstract class Resource[T] extends LocalCleanable {

  import Resource._

  @volatile protected var existingStream: T = _
  protected def _stream: T
  final lazy val stream = Option(existingStream).getOrElse {
    existingStream = _stream
    existingStream
  }

  def getURI: String
  def getName: String

  def getType: String = UNKNOWN
  final lazy val isDirectory: Boolean = getType == DIR

  def getContentType: String
  def getLenth: Long
  def getStatusCode: Option[Int] = None

  def getLastModified: Long

  def isAlreadyExisting: Boolean

  protected def _metadata: ResourceMD

  lazy val rootMetadata: ResourceMD = {
    val reflective = Resource.resource2MD(this)
    reflective ++ _metadata
  }

  def children: Seq[ResourceMD] = Nil

  lazy val allMetadata: ResourceMD = {

    val grouped = children.groupBy(_.map("Type").toString)
    val childMaps: Map[String, Seq[ListMap[String, Any]]] = grouped.mapValues {
      _.map {
        md =>
          md.map
      }
    }
    val result = rootMetadata ++ ResourceMD.MapParser(childMaps)
    result
  }

  //  override def cleanImpl(): Unit = {}
}

abstract class InputResource extends Resource[InputStream] {

  override def cleanImpl(): Unit = Option(existingStream)
    .foreach(_.close)
}
abstract class OutputResource extends Resource[OutputStream] {

  override def cleanImpl(): Unit = Option(existingStream)
    .foreach{
      v =>
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

  val resource2MD = Metadata.ReflectionParser[Resource[_]]()

  final val DIR = "directory"
  final val DIR_MIME = "inode/directory; charset=UTF-8"
  final val UNKNOWN = "unknown"

  implicit def view(self: ResourceMD) = new ResourceMDView(self)
}
