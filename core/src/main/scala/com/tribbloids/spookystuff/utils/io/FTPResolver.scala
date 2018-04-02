package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.net.{URI, URLConnection}

import com.tribbloids.spookystuff.utils.http.HttpUtils
import org.apache.spark.ml.dsl.utils.metadata.MetadataMap

object FTPResolver {

  def apply(
             timeoutMillis: Int
           ): FTPResolver = {

    val ftp = FTPResolver({
      uri =>
        val uc = uri.toURL.openConnection()
        uc.setConnectTimeout(timeoutMillis)
        uc.setReadTimeout(timeoutMillis)
        uc
    })

    ftp
  }
}

case class FTPResolver(
                        input2Connection: URI => URLConnection
                      ) extends URIResolver {

  override def input[T](pathStr: String)(f: InputStream => T) = {

    val uri = HttpUtils.uri(pathStr)
    val uc = input2Connection(uri)
    uc.connect()
    val is = uc.getInputStream

    try {
      val result = f(is)

      SimpleResource[T](
        result,
        {
          import Resource._

          MetadataMap(
            URI_ -> pathStr,
            CONTENT_TYPE -> uc.getContentType,
            LENGTH -> uc.getContentLength
          )
        }
      )
    }
    finally {
      is.close()
    }
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T) = {
    val uri = HttpUtils.uri(pathStr)
    val uc = input2Connection(uri)
    uc.connect()
    val os = uc.getOutputStream

    try {
      val result = f(os)
      os.flush()

      SimpleResource(
        result,
        {
          import Resource._

          MetadataMap(
            URI_ -> pathStr,
            CONTENT_TYPE -> uc.getContentType,
            LENGTH -> uc.getContentLength
          )
        }
      )
    }
    finally {
      os.close()
    }
  }
}
