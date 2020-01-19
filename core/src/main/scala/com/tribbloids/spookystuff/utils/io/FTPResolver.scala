package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.Retry
import com.tribbloids.spookystuff.utils.http.HttpUtils

import java.io.{InputStream, OutputStream}
import java.net.{URI, URLConnection}

object FTPResolver {

  def apply(
      timeoutMillis: Int
  ): FTPResolver = {

    val ftp = FTPResolver({ uri =>
      val conn = uri.toURL.openConnection()
      conn.setConnectTimeout(timeoutMillis)
      conn.setReadTimeout(timeoutMillis)

      conn
    })

    ftp
  }
}

case class FTPResolver(
    input2Connection: URI => URLConnection,
    override val retry: Retry = Retry.ExponentialBackoff(8, 16000)
) extends URIResolver {

  import scala.collection.JavaConverters._

  override def newExecution(pathStr: String): Execution = new Execution(pathStr)
  case class Execution(pathStr: String) extends super.AbstractExecution {

    override def absolutePathStr: String = pathStr

    val uri: URI = HttpUtils.uri(pathStr)
    val _conn: URLConnection = input2Connection(uri)

    trait FTPResource[T] extends Resource[T] with MimeTypeMixin {

      lazy val conn: URLConnection = {
        _conn.connect()
        _conn
      }

      override lazy val getURI: String = pathStr

      override lazy val getName: String = null

      override lazy val getContentType: String = conn.getContentType

      override lazy val getLength: Long = conn.getContentLengthLong

      override lazy val getLastModified: Long = conn.getLastModified

      override lazy val _metadata: ResourceMetadata = {
        val map = conn.getHeaderFields.asScala.toMap
          .mapValues { _list =>
            val list = _list.asScala
            val result =
              if (list.size == 1) list.head
              else list
            result.asInstanceOf[Any]
          }
        ResourceMetadata.fromMap(map)
      }
    }

    override def input[T](fn: InputResource => T): T = {

      val in = new InputResource with FTPResource[InputStream] {

        override def createStream: InputStream = {
          conn.getInputStream
        }

      }
      try {
        fn(in)
      } finally {
        in.clean()
      }
    }

    override def output[T](mode: WriteMode)(fn: OutputResource => T): T = {
      val out: OutputResource with FTPResource[OutputStream] = new OutputResource with FTPResource[OutputStream] {

        override val createStream: OutputStream = {
          conn.getOutputStream
        }
      }
      try {
        fn(out)
      } finally {
        out.clean()
      }
    }

    override def _delete(mustExist: Boolean): Unit = {

      unsupported("delete")
      //TODO: not supported by java library! should switch to a more professional one like org.apache.commons.net.ftp.FTPClient
    }

    override def moveTo(target: String): Unit = {

      unsupported("move")
    }

//    override def mkDirs(): Unit = ???
  }
}
