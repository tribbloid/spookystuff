package com.tribbloids.spookystuff.utils.io

import java.io.{InputStream, OutputStream}
import java.net.URI

import com.tribbloids.spookystuff.session.WebProxySetting
import com.tribbloids.spookystuff.utils.NOTSerializable
import com.tribbloids.spookystuff.utils.http._
import org.apache.hadoop.conf.Configuration
import org.apache.http.client.methods.HttpRequestBase

trait CompoundResolver extends URIResolver {

  def getImpl(uri: String): URIResolver

  override def input[T](pathStr: String)(f: InputStream => T) =
    getImpl(pathStr).input(pathStr)(f)

  override def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T) =
    getImpl(pathStr).output(pathStr, overwrite)(f)

  override def lockAccessDuring[T](pathStr: String)(f: String => T) =
    getImpl(pathStr).lockAccessDuring(pathStr)(f)
}

class FSResolver(
                  hadoopConf: Configuration,
                  timeoutMillis: Int
                ) extends CompoundResolver {

  lazy val hdfs = HDFSResolver(hadoopConf)

  lazy val ftp = FTPResolver({
    uri =>
      val uc = uri.toURL.openConnection()
      uc.setConnectTimeout(timeoutMillis)
      uc.setReadTimeout(timeoutMillis)
      uc
  })

  override def getImpl(uri: String): URIResolver = {

    val _uri = HttpUtils.uri(uri)
    val scheme = _uri.getScheme
    scheme match {
      case "ftp" | "ftps" =>
        ftp
      case "local" =>
        LocalResolver //TODO: useless? identical to "file://"
      case _ =>
        hdfs
    }
  }
}

class OmniResolver(
                    hadoopConf: Configuration,
                    timeoutMillis: Int,
                    webProxy: WebProxySetting,
                    input2Http: URI => HttpRequestBase
                  )extends FSResolver(hadoopConf, timeoutMillis) with NOTSerializable {

  override def getImpl(uri: String): URIResolver = {

    val _uri = HttpUtils.uri(uri)
    val scheme = _uri.getScheme
    scheme match {
      case "http" | "https" =>
        http
      case _ =>
        super.getImpl(uri)
    }
  }

  lazy val http = HTTPResolver(timeoutMillis, webProxy, input2Http)
}