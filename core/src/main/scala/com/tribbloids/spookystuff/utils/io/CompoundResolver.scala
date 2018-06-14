package com.tribbloids.spookystuff.utils.io

import java.net.URI

import com.tribbloids.spookystuff.session.WebProxySetting
import com.tribbloids.spookystuff.utils.http._
import com.tribbloids.spookystuff.utils.{NOTSerializable, SerBox}
import org.apache.hadoop.conf.Configuration
import org.apache.http.client.methods.HttpRequestBase

trait CompoundResolver extends URIResolver {

  def getImpl(pathStr: String): URIResolver

  override def Execution(pathStr: String) = new Execution(pathStr)
  case class Execution(pathStr: String) extends super.Execution {

    lazy val impl = getImpl(pathStr).Execution(pathStr)

    override def absolutePathStr: String = impl.absolutePathStr

    override def input[T](f: InputResource => T) =
      impl.input(f)

    override def output[T](overwrite: Boolean)(f: OutputResource => T) =
      impl.output(overwrite)(f)

    override def _remove(mustExist: Boolean): Unit = impl._remove(mustExist)
  }

//  override def lockAccessDuring[T](pathStr: String)(f: String => T) =
//    getImpl(pathStr).lockAccessDuring(pathStr)(f)
}

class FSResolver(
                  hadoopConf: SerBox[Configuration],
                  timeoutMillis: Int
                ) extends CompoundResolver {

  lazy val hdfs = HDFSResolver(hadoopConf)

  lazy val ftp = FTPResolver(timeoutMillis)

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
                    hadoopConf: SerBox[Configuration],
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