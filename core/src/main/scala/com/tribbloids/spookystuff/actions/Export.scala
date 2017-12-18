package com.tribbloids.spookystuff.actions

import java.net.URI
import java.util.Date

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.dsl.DocFilters
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.extractors.{Col, Extractor, FR}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.utils.http._
import com.tribbloids.spookystuff.utils.io._
import org.apache.commons.io.IOUtils
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.dsl.utils.metadata.MetadataMap
import org.openqa.selenium.{OutputType, TakesScreenshot}

/**
  * Export a page from the browser or http client
  * the page an be anything including HTML/XML file, image, PDF file or JSON string.
  */
@SerialVersionUID(564570120183654L)
abstract class Export extends Named {

  def filter: DocFilter = DocFilters.Bypass

  final override def outputNames = Set(this.name)

  final override def skeleton = None //have not impact to driver

  final def doExe(session: Session): Seq[DocOption] = {
    val results = doExeNoName(session)
    results.map{
      case doc: Doc =>
        try {
          filter.apply(doc, session)
        }
        catch {
          case e: Throwable =>
            val message = getSessionExceptionMessage(session, Some(doc))
            val wrapped = DocWithError(doc, message, e)

            throw wrapped
        }
      case other: DocOption =>
        other
    }
  }

  def doExeNoName(session: Session): Seq[DocOption]
}


trait Wayback {

  def wayback: Extractor[Long]
}

trait WaybackSupport extends Wayback {

  var wayback: Extractor[Long] = _

  def waybackTo(date: Extractor[Date]): this.type = {
    this.wayback = date.andFn(_.getTime)
    this
  }

  def waybackTo(date: Date): this.type = this.waybackTo(Lit(date))

  def waybackToTimeMillis(time: Extractor[Long]): this.type = {
    this.wayback = time
    this
  }

  def waybackToTimeMillis(date: Long): this.type = this.waybackToTimeMillis(Lit(date))

  //has to be used after copy
  protected def injectWayback(
                               wayback: Extractor[Long],
                               pageRow: FetchedRow,
                               schema: SpookySchema
                             ): Option[this.type] = {
    if (wayback == null) Some(this)
    else {
      val valueOpt = wayback.resolve(schema).lift(pageRow)
      valueOpt.map{
        v =>
          this.wayback = Lit.erased(v)
          this
      }
    }
  }
}

/**
  * Export the current page from the browser
  * interact with the browser to load the target page first
  * only for html page, please use wget for images and pdf files
  * always export as UTF8 charset
  */
case class Snapshot(
                     override val filter: DocFilter = Const.defaultDocumentFilter
                   ) extends Export with WaybackSupport{

  // all other fields are empty
  override def doExeNoName(session: Session): Seq[Doc] = {

    val pageOpt = session.webDriverOpt.map {
      webDriver =>
        new Doc(
          DocUID((session.backtrace :+ this).toList, this)(),
          webDriver.getCurrentUrl,
          webDriver.getPageSource.getBytes("UTF8"),
          Some("text/html; charset=UTF-8")
          //      serializableCookies
        )
    }
    //    if (contentType != null) Seq(page.copy(declaredContentType = Some(contentType)))
    pageOpt.map(v => Seq(v)).getOrElse(Nil)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema) = {
    this.copy().asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
  }
}

//this is used to save GC when invoked by anothor component
object QuickSnapshot extends Snapshot(DocFilters.Bypass)
object ErrorDump extends Snapshot(DocFilters.Bypass)
  //  with MessageAPI
{

  //  override def proto = "ErrorDump"
}

case class Screenshot(
                       override val filter: DocFilter = Const.defaultImageFilter
                     ) extends Export with WaybackSupport {

  override def doExeNoName(session: Session): Seq[Doc] = {

    val pageOpt = session.webDriverOpt.map {
      webDriver =>
        val content = webDriver.self match {
          case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
          case _ => throw new UnsupportedOperationException("driver doesn't support screenshot")
        }

        val page = new Doc(
          DocUID((session.backtrace :+ this).toList, this)(),
          webDriver.getCurrentUrl,
          content,
          Some("image/png")
        )
        page
    }

    pageOpt.map(v => Seq(v)).getOrElse(Nil)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema) = {
    this.copy().asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
  }
}

object QuickScreenshot extends Screenshot(DocFilters.Bypass)
object ErrorScreenshot extends Screenshot(DocFilters.Bypass)
  //  with MessageAPI
{

  //  override def proto = "ErrorScreenshot"
}

//TODO: handle RedirectException for too many redirections.
//@SerialVersionUID(7344992460754628988L)
abstract class HttpMethod(
                           uri: Col[String]
                         ) extends Export with Driverless with Timed with WaybackSupport {

  @transient lazy val uriOption: Option[URI] = {
    val uriStr = uri.value.trim()
    if ( uriStr.isEmpty ) None
    else Some(HttpUtils.uri(uriStr))
  }

  def resolveURI(pageRow: FetchedRow, schema: SpookySchema): Option[Lit[FR, String]] = {
    val first = this.uri.resolve(schema).lift(pageRow)
      .flatMap(SpookyUtils.asOption[Any])
    //TODO: no need to resolve array output?

    val uriStr: Option[String] = first.flatMap {
      case element: Unstructured => element.href
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case _ => None
    }
    val uriLit = uriStr.map(Lit.erased[String])
    uriLit
  }
}

/**
  * use an http GET to fetch a remote resource deonted by url
  * http client is much faster than browser, also load much less resources
  * recommended for most static pages.
  * actions for more complex http/restful API call will be added per request.
  *
  * @param uri support cell interpolation
  */
@SerialVersionUID(-8687280136721213696L)
case class Wget(
                 uri: Col[String],
                 override val filter: DocFilter = Const.defaultDocumentFilter
               ) extends HttpMethod(uri) {

  def getResolver(session: Session) = {
    val hadoopConf = session.spooky.hadoopConf
    val timeout = this.timeout(session).toMillis.toInt
    val proxy = session.spooky.spookyConf.webProxy()

    val resolver = new OmniResolver(
      hadoopConf,
      timeout,
      proxy,
      {
        uri =>
          val headers = session.spooky.spookyConf.httpHeadersFactory()

          val request = new HttpGet(uri)
          for (pair <- headers) {
            request.addHeader(pair._1, pair._2)
          }

          request
      }
    )
    resolver
  }

  override def doExeNoName(session: Session): Seq[DocOption] = {

    val resolver = getResolver(session)

    val _uri = uri.value

    val resource: Resource[Array[Byte]] = resolver.input(_uri) {
      is =>
        IOUtils.toByteArray(is)
    }

    val md: ResourceMD = resource.metadata

    import Resource._

    val cacheLevel = DocCacheLevel.getDefault(uriOption)
    val doc = if (md.IS_DIR.get.getOrElse(false)) {
      val xmlStr = md.toXMLStr()

      new Doc(
        uid = DocUID(List(this), this)(),
        uri = md.URI_(),
        raw = xmlStr.getBytes("utf-8"),
        declaredContentType = Some("inode/directory; charset=UTF-8"),
        cacheLevel = cacheLevel,
        metadata = md
      )
    }
    else {

      import Resource._

      new Doc(
        uid = DocUID(List(this), this)(),
        uri = md.URI_(),
        raw = resource.value,
        cacheLevel = cacheLevel,
        metadata = md
      )
    }
    Seq(doc)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val uriLit: Option[Lit[FR, String]] = resolveURI(pageRow, schema)

    uriLit.flatMap(
      lit =>
        this.copy(uri = lit).asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
    )
  }
}

object Wpost{

  def apply(
             uri: Col[String],
             filter: DocFilter = Const.defaultDocumentFilter,
             entity: HttpEntity = new StringEntity("")
           ): WpostImpl = WpostImpl(uri, filter)(entity)

}

@SerialVersionUID(2416628905154681500L)
case class WpostImpl private[actions](
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
            IOUtils.toString(v.getContent)
        text
      case _ => entity.toString
    }
    txt + "\n"
  }

  def getResolver(session: Session) = {

    val timeout = this.timeout(session).toMillis.toInt
    val hadoopConf = session.spooky.sparkContext.hadoopConfiguration
    val proxy = session.spooky.spookyConf.webProxy()

    val resolver = new OmniResolver(
      hadoopConf,
      timeout,
      proxy,
      {
        uri: URI =>
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
        val resource: Resource[Array[Byte]] = v.input(uri){
          is =>
            IOUtils.toByteArray(is)
        }

        val md = resource.metadata

        import Resource._

        val cacheLevel = DocCacheLevel.getDefault(uriOption)
        new Doc(
          uid = DocUID(List(this), this)(),
          uri = md.URI_(),
          raw = resource.value,
          cacheLevel = cacheLevel,
          metadata = md
        )
      case _ =>
        val resource: Resource[Int] = impl.output(uri, overwrite = true){
          os =>
            IOUtils.copy(entity.getContent, os)
        }

        import Resource._

        val md: ResourceMD = resource.metadata.map ++ MetadataMap(LENGTH -> resource.value)

        NoDoc(
          backtrace = List(this),
          cacheLevel = DocCacheLevel.NoCache,
          metadata = md
        )
    }
    Seq(doc)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val uriLit: Option[Lit[FR, String]] = resolveURI(pageRow, schema)

    uriLit.flatMap(
      lit =>
        this.copy(uri = lit)(entity).asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
    )
  }
}
