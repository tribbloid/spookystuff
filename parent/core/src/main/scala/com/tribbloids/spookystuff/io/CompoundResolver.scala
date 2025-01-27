package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.agent.WebProxySetting
import com.tribbloids.spookystuff.utils.http.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpRequestBase

import java.net.URI
import scala.language.reflectiveCalls

trait CompoundResolver extends URIResolver {

  def getImpl(uri: PathMagnet.URIPath): URIResolver

  implicit class _Execution(uri: PathMagnet.URIPath) extends Execution {

    lazy val impl: URIExecution = getImpl(uri).execute(uri)

    override def absolutePath: PathMagnet.URIPath = impl.absolutePath

    override def _delete(mustExist: Boolean): Unit = impl.delete(mustExist)

    override def moveTo(target: String, force: Boolean = false): Unit = impl.moveTo(target, force)

    //    override def mkDirs(): Unit = impl.mkDirs()
    override type _Resource = impl._Resource

    override def _Resource: WriteMode => impl._Resource = { v =>
      impl._Resource(v)
    }
  }

  //  override def lockAccessDuring[T](pathStr: String)(f: String => T) =
  //    getImpl(pathStr).lockAccessDuring(pathStr)(f)
}

object CompoundResolver {

  // TODO: should be "FileService"
  class FSResolver(
      hadoopConfFactory: () => Configuration,
      timeoutMillis: Int
  ) extends CompoundResolver {

    lazy val hdfs: HDFSResolver = HDFSResolver(hadoopConfFactory)

    lazy val ftp: URLConnectionResolver = URLConnectionResolver(timeoutMillis)

    override def getImpl(uri: PathMagnet.URIPath): URIResolver = {

      val _uri = HttpUtils.uri(uri)
      val scheme = _uri.getScheme
      scheme match {
        case "ftp" | "ftps" =>
          ftp
        case "local" =>
          LocalResolver // TODO: useless? identical to "file://"
        case _ =>
          hdfs
      }
    }
  }

  // TODO: should be "Omni"
  class OmniResolver(
      hadoopConfFactory: () => Configuration,
      timeoutMillis: Int,
      webProxy: WebProxySetting,
      input2Http: URI => HttpRequestBase
  ) extends FSResolver(hadoopConfFactory, timeoutMillis)
      with NOTSerializable {

    override def getImpl(uri: PathMagnet.URIPath): URIResolver = {

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
}
