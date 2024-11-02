package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.io.CompoundResolver.OmniResolver
import com.tribbloids.spookystuff.io.{HTTPResolver, ResourceMetadata, WriteMode}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.shaded.org.apache.http.HttpEntity
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity

import java.net.URI

object Wpost {

  def apply(
      uri: String,
      filter: DocFilter = Const.defaultDocumentFilter,
      entity: HttpEntity = new StringEntity("")
  ): WpostImpl = WpostImpl(uri, filter)(entity)

  @SerialVersionUID(2416628905154681500L)
  case class WpostImpl(
      uri: String,
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

    def getResolver(agent: Agent): OmniResolver = {

      val timeout = this.timeout(agent).max.toMillis.toInt
      val hadoopConf = agent.spooky.hadoopConf
      val proxy = agent.spooky.conf.webProxy {}

      val resolver = new OmniResolver(
        () => hadoopConf,
        timeout,
        proxy,
        { (uri: URI) =>
          val headers = agent.spooky.conf.httpHeadersFactory.function0.apply()

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

    override def doExeNoName(agent: Agent): Seq[Observation] = {

      val resolver = getResolver(agent)
      val impl = resolver.getImpl(uri)

      val doc = impl match {
        case v: HTTPResolver =>
          v.input(uri) { in =>
            val md = in.metadata.root
            val cacheLevel = DocCacheLevel.getDefault(uriOption)

            Doc(
              uid = DocUID(List(this), this)(),
              uri = in.getURI,
              cacheLevel = cacheLevel,
              metadata = md
            )().setRaw(IOUtils.toByteArray(in.stream))

          }

        case _ =>
          impl.output(uri, WriteMode.Overwrite) { out =>
            val length = IOUtils.copy(entity.getContent, out.stream)

            val md: ResourceMetadata = out.metadata.root.updated("length" -> length)
            NoDoc(
              backtrace = List(this),
              cacheLevel = DocCacheLevel.NoCache,
              metadata = md
            )
          }
      }
      Seq(doc)
    }
  }

}
