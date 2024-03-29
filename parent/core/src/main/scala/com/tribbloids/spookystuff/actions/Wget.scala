package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.extractors.{Col, FR}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.utils.io.CompoundResolver.OmniResolver
import org.apache.commons.io.IOUtils
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpGet

/**
  * use an http GET to fetch a remote resource deonted by url http client is much faster than browser, also load much
  * less resources recommended for most static pages. actions for more complex http/restful API call will be added per
  * request.
  *
  * @param uri
  *   support cell interpolation
  */
@SerialVersionUID(-8687280136721213696L)
case class Wget(
    uri: Col[String],
    override val filter: DocFilter = Const.defaultDocumentFilter
) extends HttpMethod(uri) {

  def getResolver(agent: Agent): OmniResolver = {

    val timeout = this.timeout(agent).max.toMillis.toInt
    val hadoopConf = agent.spooky.hadoopConf
    val proxy = agent.spooky.conf.webProxy {}

    val resolver = new OmniResolver(
      () => hadoopConf,
      timeout,
      proxy,
      { uri =>
        val headers = agent.spooky.conf.httpHeadersFactory()

        val request = new HttpGet(uri)
        for (pair <- headers) {
          request.addHeader(pair._1, pair._2)
        }

        request
      }
    )
    resolver
  }

  override def doExeNoName(agent: Agent): Seq[Observation] = {

    val resolver = getResolver(agent)
    val _uri = uri.value

    val cacheLevel = DocCacheLevel.getDefault(uriOption)
    val doc = resolver.input(_uri) { in =>
      if (in.isDirectory) {
        val xmlStr = in.metadata.all.toXMLStr()

        Doc(
          uid = DocUID(List(this), this)(),
          uri = in.getURI,
          declaredContentType = Some("inode/directory; charset=UTF-8"),
          cacheLevel = cacheLevel,
          metadata = in.metadata.root
        )().setRaw(xmlStr.getBytes("utf-8"))
      } else {

        val raw = IOUtils.toByteArray(in.stream)

        Doc(
          uid = DocUID(List(this), this)(),
          uri = in.getURI,
          cacheLevel = cacheLevel,
          metadata = in.metadata.root
        )().setRaw(raw)
      }
    }
    Seq(doc)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val uriLit: Option[Lit[FR, String]] = resolveURI(pageRow, schema)

    uriLit.flatMap(lit => this.copy(uri = lit).asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema))
  }
}
