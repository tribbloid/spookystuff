package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.net.{URI, URLConnection}

import com.tribbloids.spookystuff.utils.http.HttpUtils

import scala.util.Try

object FTPResolver {

  def apply(
             timeoutMillis: Int
           ): FTPResolver = {

    val ftp = FTPResolver({
      uri =>
        val conn = uri.toURL.openConnection()
        conn.setConnectTimeout(timeoutMillis)
        conn.setReadTimeout(timeoutMillis)
        conn
    })

    ftp
  }
}

case class FTPResolver(
                        input2Connection: URI => URLConnection
                      ) extends URIResolver {

  import scala.collection.JavaConverters._

  override def Execution(pathStr: String) = new Execution(pathStr)
  case class Execution(pathStr: String) extends super.Execution {

    override def absolutePathStr: String = pathStr

    val uri = HttpUtils.uri(pathStr)
    val _conn: URLConnection = input2Connection(uri)

    trait FTPResource[T] extends Resource[T] {

      lazy val conn = {
        _conn.connect()
        _conn
      }

      override lazy val getURI: String = pathStr

      override lazy val getName: String = null

      override lazy val getContentType: String = conn.getContentType

      override lazy val getLenth: Long = conn.getContentLengthLong

      override lazy val getLastModified: Long = conn.getLastModified

      override def isAlreadyExisting: Boolean = Try{conn}.isSuccess

      override lazy val _metadata: ResourceMD = {
        val map = conn.getHeaderFields.asScala
          .mapValues {
            _list =>
              val list = _list.asScala
              val result = if (list.size == 1) list.head
              else list
              result.asInstanceOf[Any]
          }
        ResourceMD.apply(map.toSeq: _*)
      }
    }

    override def input[T](f: InputResource => T): T = {

      val in = new InputResource with FTPResource[InputStream] {

        override def _stream: InputStream = {
          conn.getInputStream
        }
      }
      try {
        f(in)
      }
      finally {
        in.close()
      }
    }

    override def remove(mustExist: Boolean): Unit = {
      ???
      //TODO: not supported by java library! should switch to a more professional one like org.apache.commons.net.ftp.FTPClient
    }

    override def output[T](overwrite: Boolean)(f: OutputResource => T): T = {
      val out = new OutputResource with FTPResource[OutputStream] {

        override val _stream: OutputStream = {
          conn.getOutputStream
        }
      }
      try {
        f(out)
      }
      finally {
        out.close()
      }
    }
  }
}
