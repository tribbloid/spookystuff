package com.tribbloids.spookystuff.actions

import java.net.{InetSocketAddress, URI}
import java.util.Date
import javax.net.ssl.SSLContext

import com.tribbloids.spookystuff.dsl.ExportFilter
import com.tribbloids.spookystuff.entity.PageRow
import com.tribbloids.spookystuff.expressions.{Expression, Literal}
import com.tribbloids.spookystuff.http._
import com.tribbloids.spookystuff.pages._
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.{HDFSResolver, LocalResolver, Utils}
import com.tribbloids.spookystuff.{Const, ExportFilterException, QueryException}
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpUriRequest}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.client.{ClientProtocolException, RedirectException}
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.protocol.HttpCoreContext
import org.apache.http.{HttpHost, StatusLine}
import org.openqa.selenium.{OutputType, TakesScreenshot}

/**
 * Export a page from the browser or http client
 * the page an be anything including HTML/XML file, image, PDF file or JSON string.
 */


abstract class Export extends Named with Wayback{

  def filter: ExportFilter

  final override def outputNames = Set(this.name)

  final override def trunk = None //have not impact to driver

  final def doExe(session: Session) = {
    val results = doExeNoName(session)
    results.map{
      case page: Page =>
        try {
          filter.apply(page, session)
        }
        catch {
          case e: Throwable =>
            var message = "\n\n+>" + this.toString

            val errorDump = session.spooky.conf.errorDump

            if (errorDump) {
              message += "\nSnapshot: " +this.errorDump(message, page, session.spooky)
            }

            throw new ExportFilterException(message, e)
        }
      case other: PageLike =>
        other
    }
  }

  def doExeNoName(session: Session): Seq[PageLike]
}

trait WaybackSupport {
  self: Wayback =>

  import com.tribbloids.spookystuff.dsl._

  var wayback: Expression[Long] = null

  def waybackTo(date: Expression[Date]): this.type = {
    this.wayback = date.andMap(_.getTime)
    this
  }

  def waybackTo(date: Date): this.type = this.waybackTo(Literal(date))

  def waybackToTimeMillis(time: Expression[Long]): this.type = {
    this.wayback = time
    this
  }

  def waybackToTimeMillis(date: Long): this.type = this.waybackToTimeMillis(Literal(date))

  protected def interpolateWayback(pageRow: PageRow): Option[this.type] = {
    if (this.wayback == null) Some(this)
    else {
      val valueOpt = this.wayback(pageRow)
      valueOpt.map{
        v =>
          this.wayback = Literal(v)
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
                     override val filter: ExportFilter = Const.defaultDocumentFilter,
                     contentType: String = null
                     ) extends Export with WaybackSupport{

  // all other fields are empty
  override def doExeNoName(pb: Session): Seq[Page] = {

    //    import scala.collection.JavaConversions._

    //    val cookies = pb.driver.manage().getCookies
    //    val serializableCookies = ArrayBuffer[SerializableCookie]()
    //
    //    for (cookie <- cookies) {
    //      serializableCookies += cookie.asInstanceOf[SerializableCookie]
    //    }

    val page = new Page(
      PageUID(pb.backtrace :+ this, this),
      pb.driver.getCurrentUrl,
      Some("text/html; charset=UTF-8"),
      pb.driver.getPageSource.getBytes("UTF8")
      //      serializableCookies
    )

    if (contentType != null) Seq(page.copy(declaredContentType = Some(contentType)))
    else Seq(page)
  }

  override def doInterpolate(pageRow: PageRow) = {
    this.copy().asInstanceOf[this.type].interpolateWayback(pageRow)
  }
}

//this is used to save GC when invoked by anothor component
object DefaultSnapshot extends Snapshot()

case class Screenshot(
                       override val filter: ExportFilter = Const.defaultImageFilter
                       ) extends Export with WaybackSupport {

  override def doExeNoName(pb: Session): Seq[Page] = {

    val content = pb.driver match {
      case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
      case _ => throw new UnsupportedOperationException("driver doesn't support snapshot")
    }

    val page = new Page(
      PageUID(pb.backtrace :+ this, this),
      pb.driver.getCurrentUrl,
      Some("image/png"),
      content
    )

    Seq(page)
  }

  override def doInterpolate(pageRow: PageRow) = {
    this.copy().asInstanceOf[this.type].interpolateWayback(pageRow)
  }
}

object DefaultScreenshot extends Screenshot()

/**
 * use an http GET to fetch a remote resource deonted by url
 * http client is much faster than browser, also load much less resources
 * recommended for most static pages.
 * actions for more complex http/restful API call will be added per request.
 * @param uri support cell interpolation
 */
case class Wget(
                 uri: Expression[Any],
                 override val filter: ExportFilter = Const.defaultDocumentFilter,
                 contentType: String = null
                 ) extends Export with Driverless with Timed with WaybackSupport {

  lazy val uriOption: Option[URI] = {
    val uriStr = uri.asInstanceOf[Literal[String]].value.trim()
    if ( uriStr.isEmpty ) None
    else Some(HttpUtils.uri(uriStr))
  }

  //  def effectiveURIString = uriOption.map(_.toString)

  override def doExeNoName(session: Session): Seq[PageLike] = {

    uriOption match {
      case None => Nil
      case Some(uriURI) =>
        val result = Option(uriURI.getScheme).getOrElse("file") match {
          case "http" | "https" =>
            getHttp(uriURI, session)
          case "ftp" =>
            getFtp(uriURI, session)
          case "file" =>
            getLocal(uriURI, session)
          case _ =>
            getHDFS(uriURI, session)
        }
        if (this.contentType != null) result.map{
          case page: Page => page.copy(declaredContentType = Some(this.contentType))
          case others: PageLike => others
        }
        else result
    }
  }

  //DEFINITELY NOT CACHED
  def getLocal(uri: URI, session: Session): Seq[PageLike] = {

    val pathStr = uri.toString.replaceFirst("file://","")

    val content = LocalResolver.input(pathStr) {
      fis =>
        IOUtils.toByteArray(fis)
    }

    val result = new Page(
      PageUID(Seq(this), this),
      uri.toString,
      None,
      content,
      cacheable = false
    )

    Seq(result)
  }

  //not cached
  def getHDFS(uri: URI, session: Session): Seq[PageLike] = {
    val content = HDFSResolver(session.spooky.hadoopConf).input(uri.toString) {
      fis =>
        IOUtils.toByteArray(fis)
    }

    val result = new Page(
      PageUID(Seq(this), this),
      uri.toString,
      None,
      content,
      cacheable = false
    )

    Seq(result)
  }

  def getFtp(uri: URI, session: Session): Seq[PageLike] = {

    val timeoutMs = this.timeout(session).toMillis.toInt

    val uc = uri.toURL.openConnection()
    uc.setConnectTimeout(timeoutMs)
    uc.setReadTimeout(timeoutMs)

    uc.connect()
    uc.getInputStream
    val stream = uc.getInputStream

    val content = IOUtils.toByteArray ( stream )

    val result = new Page(
      PageUID(Seq(this), this),
      uri.toString,
      None,
      content
    )

    Seq(result)
  }

  def getHttp(uri: URI, session: Session): Seq[PageLike] = {

    val proxy = session.spooky.conf.proxy()
    val userAgent = session.spooky.conf.userAgent()
    val headers = session.spooky.conf.headers()
    val timeoutMs = this.timeout(session).toMillis.toInt

    val requestConfig = {

      var builder = RequestConfig.custom()
        .setConnectTimeout ( timeoutMs )
        .setConnectionRequestTimeout ( timeoutMs )
        .setSocketTimeout( timeoutMs )
        .setRedirectsEnabled(true)
        .setCircularRedirectsAllowed(true)
        .setRelativeRedirectsAllowed(true)
        .setAuthenticationEnabled(false)
      //        .setCookieSpec(CookieSpecs.BEST_MATCH)

      if (proxy!=null && !proxy.protocol.startsWith("socks")) builder=builder.setProxy(new HttpHost(proxy.addr, proxy.port, proxy.protocol))

      val result = builder.build()
      result
    }

    val sslContext: SSLContext = SSLContext.getInstance( "SSL" )
    sslContext.init(null, Array(new InsecureTrustManager()), null)
    val hostVerifier = new InsecureHostnameVerifier()

    val httpClient = if (proxy !=null && proxy.protocol.startsWith("socks")) {
      val reg = RegistryBuilder.create[ConnectionSocketFactory]
        .register("http", new SocksProxyConnectionSocketFactory())
        .register("https", new SocksProxySSLConnectionSocketFactory(sslContext))
        .build()
      val cm = new PoolingHttpClientConnectionManager(reg)

      val httpClient = HttpClients.custom
        .setConnectionManager(cm)
        .setDefaultRequestConfig ( requestConfig )
        .setRedirectStrategy(new ResilientRedirectStrategy())
        .setSslcontext(sslContext)
        .setHostnameVerifier(hostVerifier)
        .build

      httpClient
    }
    else {
      val httpClient = HttpClients.custom
        .setDefaultRequestConfig ( requestConfig )
        .setRedirectStrategy(new ResilientRedirectStrategy())
        .setSslcontext(sslContext)
        .setHostnameVerifier(hostVerifier)
        .build()

      httpClient
    }

    val request = {
      val request = new HttpGet(uri)
      if (userAgent != null) request.addHeader("User-Agent", userAgent)
      for (pair <- headers) {
        request.addHeader(pair._1, pair._2)
      }

      request
    }

    val context: HttpClientContext = if (proxy !=null && proxy.protocol.startsWith("socks")) {
      val socksaddr: InetSocketAddress = new InetSocketAddress(proxy.addr, proxy.port)
      val context: HttpClientContext = HttpClientContext.create
      context.setAttribute("socks.address", socksaddr)

      context
    }
    else HttpClientContext.create

    try {
      val response = httpClient.execute ( request, context )
      try {
        val currentReq = context.getAttribute(HttpCoreContext.HTTP_REQUEST).asInstanceOf[HttpUriRequest]
        val currentHost = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST).asInstanceOf[HttpHost]
        val currentUrl = if (currentReq.getURI.isAbsolute) {currentReq.getURI.toString}
        else {
          currentHost.toURI + currentReq.getURI
        }

        val entity = response.getEntity

        val stream = entity.getContent
        val result = try {
          val content = IOUtils.toByteArray ( stream )
          val contentType = entity.getContentType.getValue

          new Page(
            PageUID(Seq(this), this),
            currentUrl,
            Some(contentType),
            content
          )
        }
        finally {
          stream.close()
        }

        val httpStatus: StatusLine = response.getStatusLine
        assert(httpStatus.getStatusCode.toString.startsWith("2"), httpStatus.toString + "\n" + result.code)

        Seq(result)
      }
      finally {
        response.close()
      }
    }
    catch {
      case e: ClientProtocolException =>
        val cause = e.getCause
        if (cause.isInstanceOf[RedirectException]) Seq(NoPage(session.backtrace :+ this))
        else throw e
      case e: Throwable =>
        throw e
    }
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] = {
    val first = this.uri(pageRow).flatMap(Utils.encapsulateAsIterable(_).headOption)

    val uriStr: Option[String] = first.flatMap {
      case element: Unstructured => element.href
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case other => None
    }

    uriStr.flatMap(
      str =>
        this.copy(uri = new Literal(str)).interpolateWayback(pageRow).map(_.asInstanceOf[this.type])
    )
  }
}

case class OAuthV2(self: Wget) extends Export with Driverless {

  override def filter: ExportFilter = self.filter

  override def wayback: Expression[Long] = self.wayback

  def effectiveWget(session: Session): Wget = {

    val keys = session.spooky.conf.oAuthKeys.apply()
    if (keys == null) {
      throw new QueryException("need to set SpookyConf.oAuthKeys first")
    }
    val effectiveWget: Wget = self.uriOption match {
      case Some(uri) =>
        val signed = HttpUtils.OauthV2(uri.toString, keys.consumerKey, keys.consumerSecret, keys.token, keys.tokenSecret)
        self.copy(uri = Literal(signed), contentType = self.contentType)
      case None =>
        self
    }
    effectiveWget
  }

  override def doExeNoName(session: Session): Seq[PageLike] = {
    val effectiveWget = this.effectiveWget(session)

    effectiveWget.doExeNoName(session).map{
      case noPage: NoPage => noPage.copy(trace = Seq(this))
      case page: Page => page.copy(uid = PageUID(Seq(this),this))
    }
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] = self.interpolate(pageRow).map {
    v => this.copy(self = v.asInstanceOf[Wget]).asInstanceOf[this.type]
  }
}