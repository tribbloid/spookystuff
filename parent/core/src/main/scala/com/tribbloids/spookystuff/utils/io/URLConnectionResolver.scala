package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.Retry
import com.tribbloids.spookystuff.utils.http.HttpUtils

import java.io.{InputStream, OutputStream}
import java.net.{URI, URLConnection}

object URLConnectionResolver {

  def apply(
      timeoutMillis: Int
  ): URLConnectionResolver = {

    val ftp = URLConnectionResolver { uri =>
      val conn = uri.toURL.openConnection()
      conn.setConnectTimeout(timeoutMillis)
      conn.setReadTimeout(timeoutMillis)

      conn
    }

    ftp
  }
}

case class URLConnectionResolver(
    input2Connection: URI => URLConnection,
    override val retry: Retry = Retry.ExponentialBackoff(8, 16000)
) extends URIResolver {

  import scala.collection.JavaConverters._

  case class _Execution(pathStr: String) extends Execution {

    override def absolutePathStr: String = pathStr

    val uri: URI = HttpUtils.uri(pathStr)
    val _conn: URLConnection = input2Connection(uri)

    case class _Resource(mode: WriteMode) extends Resource with MimeTypeMixin {

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

      override protected def _newIStream: InputStream = {
        conn.getInputStream
      }

      override protected def _newOStream: OutputStream = {
        conn.getOutputStream
      }
    }

    override def _delete(mustExist: Boolean): Unit = {

      unsupported("delete")
      // TODO: not supported by java library! should switch to a more professional one like org.apache.commons.net.ftp.FTPClient
    }

    override def moveTo(target: String, force: Boolean = false): Unit = {

      unsupported("move")
    }

//    override def mkDirs(): Unit = ???
  }
}
