package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.NOTSerializable
import org.apache.spark.ml.dsl.utils.metadata.Metadata

import scala.language.implicitConversions

sealed class ResourceMDView(
                             val self: ResourceMD
                           ) {

  import self._

  val URI_ = Param[String]("uri")
  val NAME = Param[String]("name")
  val CONTENT_TYPE = Param[String]("content-type")
  val LENGTH = Param[Long]("length")
  val STATUS_CODE = Param[Int]("status-code")

  val IS_DIR = Param[Boolean]("isDirectory")
}

abstract class Resource[+T] extends NOTSerializable {

  def value: T

  def metadata: ResourceMD
}

object Resource extends ResourceMDView(Metadata.empty) {

  implicit def unbox[T](obj: Resource[T]): T = obj.value

  implicit def view(self: ResourceMD) = new ResourceMDView(self)
}
