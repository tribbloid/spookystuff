package com.tribbloids.spookystuff.utils.io

import java.io._
import java.lang.reflect.InvocationTargetException

import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.spark.ml.dsl.utils.messaging.MetadataRelay
import org.json4s.JsonAST.JValue

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

/*
 * to make it resilient to asynchronous read/write, let output rename the file, write it, and rename back,
 * and let input wait for file name's reversion if its renamed by another node.
 *
 * also, can ONLY resolve ABSOLUTE path! since its instances cannot be guaranteed to be in the same JVM,
 * this is the only way to guarantee that files are not affected by their respective working directory.
 */
abstract class URIResolver extends Serializable {

  def input[T](pathStr: String)(f: InputStream => T): Resource[T]

  def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T): Resource[T]

  def lockAccessDuring[T](pathStr: String)(f: String => T): T = {f(pathStr)}

  def toAbsolute(pathStr: String): String = pathStr

  final def isAbsolute(pathStr: String) = {
    toAbsolute(pathStr) == pathStr
  }

  def resourceOrAbsolute(pathStr: String): String = {
    val resourcePath = CommonUtils.getCPResource(pathStr.stripPrefix("/")).map(_.getPath).getOrElse(pathStr)

    val result = this.toAbsolute(resourcePath)
    result
  }

  //takes 10 seconds
  def DFSBlockedAccessRetries = 10
  def DFSBlockedAccessInterval = 1000

  def retry[T](f: =>T): T = {
    CommonUtils.retryFixedInterval(DFSBlockedAccessRetries, DFSBlockedAccessInterval){
      f
    }
  }

  implicit def makeMetadataSerializable(m: Map[String, _ <: Any]): Map[String, JValue] = {
    MetadataRelay.toM(m).proto
  }

  protected def reflectiveMetadata[T](status: T): ListMap[String, Any] = {
    //use reflection for all getter & boolean getter
    //TODO: move to utility
    val methods = status.getClass.getMethods
    val getters = methods.filter {
      m =>
        m.getName.startsWith("get") && (m.getParameterTypes.length == 0)
    }
      .map(v => v.getName.stripPrefix("get") -> v)
    val booleanGetters = methods.filter {
      m =>
        m.getName.startsWith("is") && (m.getParameterTypes.length == 0)
    }
      .map(v => v.getName -> v)
    val validMethods = getters ++ booleanGetters
    val kvs = validMethods.flatMap {
      tuple =>
        try {
          tuple._2.setAccessible(true)
          Some(tuple._1 -> tuple._2.invoke(status).asInstanceOf[Any])
        }
        catch {
          case e: InvocationTargetException =>
            None
        }
    }
    ListMap(kvs: _*)
  }
}

object URIResolver {
  final val DIR_TYPE = "inode/directory; charset=UTF-8"
}