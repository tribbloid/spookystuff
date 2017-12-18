package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.net.{URI, URLConnection}

import com.tribbloids.spookystuff.utils.http.HttpUtils
import org.apache.spark.ml.dsl.utils.metadata.MetadataMap

case class FTPResolver(
                        input2Connection: URI => URLConnection
                      ) extends URIResolver {

  override def input[T](pathStr: String)(f: InputStream => T) = {

    val uri = HttpUtils.uri(pathStr)
    val uc = input2Connection(uri)
    uc.connect()
    val stream = uc.getInputStream

    try {
      val result = f(stream)

      new Resource[T] {

        override val value: T = result

        override val metadata: ResourceMD = {
          import Resource._

          MetadataMap(
            URI_ -> pathStr,
            CONTENT_TYPE -> uc.getContentType,
            LENGTH -> uc.getContentLength
          )
        }
      }
    }
    finally {
      stream.close()
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

      new Resource[T] {

        override val value: T = result

        override val metadata: ResourceMD = {
          import Resource._

          MetadataMap(
            URI_ -> pathStr,
            CONTENT_TYPE -> uc.getContentType,
            LENGTH -> uc.getContentLength
          )
        }
      }
    }
    finally {
      os.close()
    }
  }
}
