package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.util.{PathMagnet, Retry}
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

  import scala.jdk.CollectionConverters.*

  implicit class _Execution(_uri: PathMagnet.URIPath) extends Execution {

    override def absolutePath = _uri

    val uri: URI = HttpUtils.uri(_uri)
    val _conn: URLConnection = input2Connection(uri)

    case class _Resource(mode: WriteMode) extends Resource with MimeTypeMixin {

      override protected def _outer: URIExecution = _Execution.this

      lazy val conn: URLConnection = {
        _conn.connect()
        _conn
      }

      override lazy val getName: String = null

      override lazy val getContentType: String = conn.getContentType

      override lazy val getLength: Long = conn.getContentLengthLong

      override lazy val getLastModified: Long = conn.getLastModified

      override lazy val extraMetadata: ResourceMetadata = {
        val map = conn.getHeaderFields.asScala.toMap.view.mapValues { _list =>
          val list = _list.asScala
          val result =
            if (list.size == 1) list.head
            else list
          result.asInstanceOf[Any]
        }.toMap
        ResourceMetadata.EAV(map)
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
