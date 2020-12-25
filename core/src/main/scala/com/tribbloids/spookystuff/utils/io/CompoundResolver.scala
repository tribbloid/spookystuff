package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.session.WebProxySetting
import com.tribbloids.spookystuff.utils.http._
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.hadoop.conf.Configuration
import org.apache.http.client.methods.HttpRequestBase

import java.net.URI

trait CompoundResolver extends URIResolver {

  def getImpl(pathStr: String): URIResolver

  override def newExecution(pathStr: String): Execution = Execution(pathStr)
  case class Execution(pathStr: String) extends super.AbstractExecution {

    lazy val impl: URIExecution = getImpl(pathStr).execute(pathStr)

    override def absolutePathStr: String = impl.absolutePathStr

    override def input[T](fn: InputResource => T): T =
      impl.input(fn)

    override def output[T](mode: WriteMode)(fn: OutputResource => T): T =
      impl.output(mode)(fn)

    override def _delete(mustExist: Boolean): Unit = impl._delete(mustExist)

    override def moveTo(target: String): Unit = impl.moveTo(target)

//    override def mkDirs(): Unit = impl.mkDirs()
  }

//  override def lockAccessDuring[T](pathStr: String)(f: String => T) =
//    getImpl(pathStr).lockAccessDuring(pathStr)(f)
}

class FSResolver(
    hadoopConfFactory: () => Configuration,
    timeoutMillis: Int
) extends CompoundResolver {

  lazy val hdfs: HDFSResolver = HDFSResolver(hadoopConfFactory)

  lazy val ftp: FTPResolver = FTPResolver(timeoutMillis)

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
    hadoopConfFactory: () => Configuration,
    timeoutMillis: Int,
    webProxy: WebProxySetting,
    input2Http: URI => HttpRequestBase
) extends FSResolver(hadoopConfFactory, timeoutMillis)
    with NOTSerializable {

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

  lazy val http: HTTPResolver = HTTPResolver(timeoutMillis, webProxy, input2Http)
}
