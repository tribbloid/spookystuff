package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.extractors.{Col, FR}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.io.CompoundResolver.OmniResolver
import com.tribbloids.spookystuff.utils.io._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.shaded.org.apache.http.HttpEntity
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity

import java.net.URI

object Wpost {

  def apply(
      uri: Col[String],
      filter: DocFilter = Const.defaultDocumentFilter,
      entity: HttpEntity = new StringEntity("")
  ): WpostImpl = WpostImpl(uri, filter)(entity)

  @SerialVersionUID(2416628905154681500L)
  case class WpostImpl private[actions] (
      uri: Col[String],
      override val filter: DocFilter
  )(
      entity: HttpEntity // TODO: cannot be dumped or serialized, fix it!
  ) extends HttpMethod(uri) {

    override def detail: String = {
      val txt = entity match {
        case v: StringEntity =>
          val text =
            v.toString + "\n" +
              IOUtils.toString(v.getContent, entity.getContentEncoding.getValue)
          text
        case _ => entity.toString
      }
      txt + "\n"
    }

    def getResolver(session: Session): OmniResolver = {

      val timeout = this.timeout(session).max.toMillis.toInt
      val hadoopConf = session.spooky.hadoopConf
      val proxy = session.spooky.spookyConf.webProxy(Unit)

      val resolver = new OmniResolver(
        () => hadoopConf,
        timeout,
        proxy,
        { uri: URI =>
          val headers = session.spooky.spookyConf.httpHeadersFactory()

          val post = new HttpPost(uri)
          for (pair <- headers) {
            post.addHeader(pair._1, pair._2)
          }
          post.setEntity(entity)

          post
        }
      )
      resolver
    }

    override def doExeNoName(session: Session): Seq[DocOption] = {

      val uri = this.uri.value

      val resolver = getResolver(session)
      val impl = resolver.getImpl(uri)

      val doc = impl match {
        case v: HTTPResolver =>
          v.input(uri) { in =>
            val md = in.metadata.root
            val cacheLevel = DocCacheLevel.getDefault(uriOption)

            new Doc(
              uid = DocUID(List(this), this)(),
              uri = in.getURI,
              raw = IOUtils.toByteArray(in.stream),
              cacheLevel = cacheLevel,
              metadata = md
            )

          }

        case _ =>
          impl.output(uri, WriteMode.Overwrite) { out =>
            val length = IOUtils.copy(entity.getContent, out.stream)
            val md: ResourceMetadata = out.metadata.root.asMap.updated("length", length)
            NoDoc(
              backtrace = List(this),
              cacheLevel = DocCacheLevel.NoCache,
              metadata = md
            )
          }
      }
      Seq(doc)
    }

    override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] = {
      val uriLit: Option[Lit[FR, String]] = resolveURI(pageRow, schema)

      uriLit.flatMap(lit =>
        this.copy(uri = lit)(entity).asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
      )
    }
  }

}
