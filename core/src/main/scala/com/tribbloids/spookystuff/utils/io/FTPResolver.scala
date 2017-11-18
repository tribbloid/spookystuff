package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.net.{URI, URLConnection}

import com.tribbloids.spookystuff.utils.http.HttpUtils

case class FTPResolver(
                        ucFactory: URI => URLConnection
                      ) extends URIResolver {

  override def input[T](pathStr: String)(f: InputStream => T) = {

    val uri = HttpUtils.uri(pathStr)
    val uc = ucFactory(uri)
    uc.connect()
    val stream = uc.getInputStream

    try {

      val result = f(stream)

      new Resource(result) {
        override val metadata = ResourceMetadata(
          pathStr,
          None,
          Option(uc.getContentType)
        )
      }
    }
    finally {
      stream.close()
    }
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T) = ???
}
