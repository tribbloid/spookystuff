package com.tribbloids.spookystuff.actions

import java.io.Closeable
import java.lang.reflect.InvocationTargetException
import java.net.{InetSocketAddress, URI, URLConnection}
import java.util.Date
import javax.net.ssl.SSLContext

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.caching.CacheLevel
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.dsl.DocFilters
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.extractors.{Extractor, FR}
import com.tribbloids.spookystuff.http._
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.{Session, WebProxySetting}
import com.tribbloids.spookystuff.utils.{HDFSResolver, SpookyUtils}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.http.client.HttpClient
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpUriRequest}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.protocol.HttpCoreContext
import org.apache.http.{HttpEntity, HttpHost, StatusLine}
import org.apache.spark.ml.dsl.utils.MessageAPI
import org.openqa.selenium.{OutputType, TakesScreenshot}

import scala.xml._

/**
  * Export a page from the browser or http client
  * the page an be anything including HTML/XML file, image, PDF file or JSON string.
  */
@SerialVersionUID(564570120183654L)
abstract class Export extends Named {

  def filter: DocFilter = DocFilters.Bypass

  final override def outputNames = Set(this.name)

  final override def skeleton = None //have not impact to driver

  final def doExe(session: Session): Seq[Fetched] = {
    val results = doExeNoName(session)
    results.map{
      case doc: Doc =>
        try {
          filter.apply(doc, session)
        }
        catch {
          case e: Throwable =>
            val message = getSessionExceptionString(session, Some(doc))
            val wrapped = DocWithError(doc, message, e)

            throw wrapped
        }
      case other: Fetched =>
        other
    }
  }

  def doExeNoName(session: Session): Seq[Fetched]
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
                               schema: DataRowSchema
                             ): Option[this.type] = {
    if (wayback == null) Some(this)
    else {
      val valueOpt = wayback.resolve(schema).lift(pageRow)
      valueOpt.map{
        v =>
          this.wayback = Lit.erase(v)
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
          Some("text/html; charset=UTF-8"),
          webDriver.getPageSource.getBytes("UTF8")
          //      serializableCookies
        )
    }
    //    if (contentType != null) Seq(page.copy(declaredContentType = Some(contentType)))
    pageOpt.map(v => Seq(v)).getOrElse(Nil)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema) = {
    this.copy().asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
  }
}

//this is used to save GC when invoked by anothor component
object QuickSnapshot extends Snapshot(DocFilters.Bypass)
object ErrorDump extends Snapshot(DocFilters.Bypass) with MessageAPI {

  override def toMessage = "ErrorDump"
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
          Some("image/png"),
          content
        )
        page
    }

    pageOpt.map(v => Seq(v)).getOrElse(Nil)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema) = {
    this.copy().asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
  }
}

object QuickScreenshot extends Screenshot(DocFilters.Bypass)
object ErrorScreenshot extends Screenshot(DocFilters.Bypass) with MessageAPI {

  override def toMessage = "ErrorScreenshot"
}

@SerialVersionUID(7344992460754628988L)
abstract class HttpMethod(
                           uri: Extractor[Any]
                         ) extends Export with Driverless with Timed with WaybackSupport {

  @transient lazy val uriOption: Option[URI] = {
    val uriStr = uri.asInstanceOf[Lit[FR, String]].toMessage.trim()
    if ( uriStr.isEmpty ) None
    else Some(HttpUtils.uri(uriStr))
  }

  def resolveURI(pageRow: FetchedRow, schema: DataRowSchema): Option[Lit[FR, String]] = {
    val first = this.uri.resolve(schema).lift(pageRow).flatMap(SpookyUtils.asArray[Any](_).headOption)
    //TODO: no need to resolve array output?

    val uriStr: Option[String] = first.flatMap {
      case element: Unstructured => element.href
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case _ => None
    }
    val uriLit = uriStr.map(Lit.erase[String])
    uriLit
  }

  def httpInvoke(
                  httpClient: HttpClient,
                  context: HttpClientContext,
                  request: HttpUriRequest,
                  cacheLevel: CacheLevel.Value = CacheLevel.All
                ): Fetched with Product = {
    val response = httpClient.execute(request, context)
    try {
      val currentReq = context.getAttribute(HttpCoreContext.HTTP_REQUEST).asInstanceOf[HttpUriRequest]
      val currentHost = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST).asInstanceOf[HttpHost]
      val currentUrl = if (currentReq.getURI.isAbsolute) {
        currentReq.getURI.toString
      }
      else {
        currentHost.toURI + currentReq.getURI
      }

      val entity: HttpEntity = response.getEntity
      val httpStatus: StatusLine = response.getStatusLine

      val stream = entity.getContent
      val result = try {
        val content = IOUtils.toByteArray(stream)
        val contentType = entity.getContentType.getValue

        new Doc(
          DocUID(List(this), this)(),
          currentUrl,
          Some(contentType),
          content,
          httpStatus = Some(httpStatus),
          cacheLevel = cacheLevel
        )
      }
      finally {
        stream.close()
      }

      result
    }
    finally {
      response match {
        case v: Closeable => v.close()
      }
    }
    //    catch {
    //      case e: ClientProtocolException =>
    //        val cause = e.getCause
    //        if (cause.isInstanceOf[RedirectException]) NoDoc(List(this)) //TODO: is it a reasonable exception? don't think so
    //        else throw e
    //      case e: Throwable =>
    //        throw e
    //    }
  }

  def getHttpClient(session: Session): (CloseableHttpClient, HttpClientContext) = {
    val proxy = session.spooky.spookyConf.webProxy()
    val timeoutMs = this.timeout(session).toMillis.toInt

    val requestConfig = {

      var builder = RequestConfig.custom()
        .setConnectTimeout(timeoutMs)
        .setConnectionRequestTimeout(timeoutMs)
        .setSocketTimeout(timeoutMs)
        .setRedirectsEnabled(true)
        .setCircularRedirectsAllowed(true)
        .setRelativeRedirectsAllowed(true)
        .setAuthenticationEnabled(false)
      //        .setCookieSpec(CookieSpecs.BEST_MATCH)

      if (proxy != null && !proxy.protocol.startsWith("socks")) builder = builder.setProxy(new HttpHost(proxy.addr, proxy.port, proxy.protocol))

      val result = builder.build()
      result
    }

    val sslContext: SSLContext = SSLContext.getInstance("SSL")
    sslContext.init(null, Array(new InsecureTrustManager()), null)
    val hostVerifier = new InsecureHostnameVerifier()

    val httpClient = if (proxy != null && proxy.protocol.startsWith("socks")) {
      val reg = RegistryBuilder.create[ConnectionSocketFactory]
        .register("http", new SocksProxyConnectionSocketFactory())
        .register("https", new SocksProxySSLConnectionSocketFactory(sslContext))
        .build()
      val cm = new PoolingHttpClientConnectionManager(reg)

      val httpClient = HttpClients.custom
        .setConnectionManager(cm)
        .setDefaultRequestConfig(requestConfig)
        .setRedirectStrategy(new ResilientRedirectStrategy())
        .setSslcontext(sslContext) //WARNING: keep until Spark get rid of httpclient 4.3
        .setHostnameVerifier(hostVerifier) //WARNING: keep until Spark get rid of httpclient 4.3
        .build

      httpClient
    }
    else {
      val httpClient = HttpClients.custom
        .setDefaultRequestConfig(requestConfig)
        .setRedirectStrategy(new ResilientRedirectStrategy())
        .setSslcontext(sslContext) //WARNING: keep until Spark get rid of httpclient 4.3
        .setHostnameVerifier(hostVerifier) //WARNING: keep until Spark get rid of httpclient 4.3
        .build()

      httpClient
    }

    val context: HttpClientContext = getHttpContext(proxy)
    (httpClient, context)
  }

  def getHttpContext(proxy: WebProxySetting): HttpClientContext = {

    val context: HttpClientContext = HttpClientContext.create

    if (proxy != null && proxy.protocol.startsWith("socks")) {
      val socksaddr: InetSocketAddress = new InetSocketAddress(proxy.addr, proxy.port)
      context.setAttribute("socks.address", socksaddr)

      context
    }
    context
  }

  def getURLConn(uri: URI, session: Session): URLConnection = {
    val timeoutMs = this.timeout(session).toMillis.toInt

    val uc = uri.toURL.openConnection()
    uc.setConnectTimeout(timeoutMs)
    uc.setReadTimeout(timeoutMs)

    uc.connect()
    uc
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
                 uri: Extractor[Any],
                 override val filter: DocFilter = Const.defaultDocumentFilter
               ) extends HttpMethod(uri) {

  override def doExeNoName(session: Session): Seq[Fetched] = {

    uriOption match {
      case None => Nil
      case Some(uriURI) =>
        val result = Option(uriURI.getScheme).getOrElse("file") match {
          case "http" | "https" =>
            getHttp(uriURI, session)
          case "ftp" =>
            getFtp(uriURI, session)
          //          case "file" =>
          //            getLocal(uriURI, session)
          case _ =>
            readHDFS(uriURI, session)
        }
        Seq(result)
    }
  }

  //DEFINITELY NOT CACHEABLE
  //  def getLocal(uri: URI, session: Session): Seq[Fetched] = {
  //
  //    val pathStr = uri.toString.replaceFirst("file://","")
  //
  //    val content = try {
  //      LocalResolver.input(pathStr) {
  //        fis =>
  //          IOUtils.toByteArray(fis)
  //      }
  //    }
  //    catch {
  //      case e: Throwable =>
  //        return Seq(
  //          NoPage(List(this), cacheable = false)
  //        )
  //    }
  //
  //    val result = new Page(
  //      PageUID(List(this), this),
  //      uri.toString,
  //      None,
  //      content,
  //      cacheable = false
  //    )
  //
  //    result
  //  }

  def readHDFS(uri: URI, session: Session): Fetched = {
    val spooky = session.spooky
    val path = new Path(uri.toString)

    val fs = path.getFileSystem(spooky.hadoopConf)

    //    if (fs.exists(path)) {
    val result: Fetched = if (fs.getFileStatus(path).isDirectory) {
      this.readHDFSDirectory(path, fs)
    }
    else {
      this.readHDFSFile(path, session)
    }
    result
    //    }
    //    else
    //      throw new FileNotFoundException(s"$uri is not a file or directory ")
  }

  def readHDFSDirectory(path: Path, fs: FileSystem): Fetched = {
    val statuses = fs.listStatus(path)
    val xmls: Array[Elem] = statuses.map {
      (status: FileStatus) =>
        //use reflection for all getter & boolean getter
        //TODO: move to utility
        val methods = status.getClass.getMethods
        val getters = methods.filter {
          m =>
            m.getName.startsWith("get") && (m.getParameterTypes.length == 0)
        }
          .map(v => v.getName.stripPrefix("get") -> v)
        val booleanGetters = methods.filter {
          m =>
            m.getName.startsWith("is") && (m.getParameterTypes.length == 0)
        }
          .map(v => v.getName -> v)
        val validMethods = getters ++ booleanGetters
        val kvs = validMethods.flatMap {
          tuple =>
            try {
              tuple._2.setAccessible(true)
              Some(tuple._1 -> tuple._2.invoke(status))
            }
            catch {
              case e: InvocationTargetException =>
                //                println(e.getCause.getMessage)
                None
            }
        }
        val nodeName = status.getPath.getName
        val attributes: Array[Attribute] = kvs.map(
          kv =>
            Attribute(null, kv._1, ""+kv._2, Null)
        )

        val node = if (status.isFile) <file>{nodeName}</file>
        else if (status.isDirectory) <directory>{nodeName}</directory>
        else if (status.isSymlink) <symlink>{nodeName}</symlink>
        else <subnode>{nodeName}</subnode>

        attributes.foldLeft(node) {
          (v1, v2) =>v1 % v2
        }
    }
    val xml = <root>{NodeSeq.fromSeq(xmls)}</root>
    val xmlStr = SpookyUtils.xmlPrinter.format(xml)

    val result: Fetched = new Doc(
      DocUID(List(this), this)(),
      path.toString,
      Some("inode/directory; charset=UTF-8"),
      xmlStr.getBytes("utf-8"),
      cacheLevel = CacheLevel.InMemory
    )
    result
  }

  //not cacheable
  def readHDFSFile(path: Path, session: Session): Fetched = {
    val content =
      HDFSResolver(session.spooky.hadoopConf).input(path.toString) {
        fis =>
          IOUtils.toByteArray(fis)
      }

    val result = new Doc(
      DocUID(List(this), this)(),
      path.toString,
      None,
      content,
      cacheLevel = CacheLevel.InMemory
    )

    result
  }

  def getFtp(uri: URI, session: Session): Fetched = {

    val uc: URLConnection = getURLConn(uri, session)
    val stream = uc.getInputStream

    try {

      val content = IOUtils.toByteArray ( stream )

      val result = new Doc(
        DocUID(List(this), this)(),
        uri.toString,
        None,
        content
      )

      result
    }
    finally {
      stream.close()
    }
  }

  def getHttp(uri: URI, session: Session): Fetched = {

    val (httpClient: CloseableHttpClient, context: HttpClientContext) = getHttpClient(session)

    val userAgent = session.spooky.spookyConf.userAgentFactory()
    val headers = session.spooky.spookyConf.headersFactory()

    val request = {
      val request = new HttpGet(uri)
      if (userAgent != null) request.addHeader("User-Agent", userAgent)
      for (pair <- headers) {
        request.addHeader(pair._1, pair._2)
      }

      request
    }

    httpInvoke(httpClient, context, request)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val uriLit: Option[Lit[FR, String]] = resolveURI(pageRow, schema)

    uriLit.flatMap(
      lit =>
        this.copy(uri = lit).asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
    )
  }
}

object Wpost{

  def apply(
             uri: Extractor[Any],
             filter: DocFilter = Const.defaultDocumentFilter,
             entity: HttpEntity = new StringEntity("")
           ): WpostImpl = WpostImpl(uri, filter)(entity)

}

@SerialVersionUID(2416628905154681500L)
case class WpostImpl private[actions](
                                       uri: Extractor[Any],
                                       override val filter: DocFilter
                                     )(
                                       entity: HttpEntity // TODO: cannot be dumped or serialized, fix it!
                                     ) extends HttpMethod(uri) {

  override def detail = {
    val txt = entity match {
      case v: StringEntity =>
        val text =
          v.toString + "\n" +
            IOUtils.toString(v.getContent)
        text
      //          .split("\n")
      //          .map(
      //            v =>
      //              "\t" + v
      //          )
      //          .mkString("\n")
      case _ => entity.toString
    }
    txt + "\n"
  }

  // not cacheable
  override def doExeNoName(session: Session): Seq[Fetched] = {

    uriOption match {
      case None => Nil
      case Some(uriURI) =>
        val result: Fetched = Option(uriURI.getScheme).getOrElse("file") match {
          case "http" | "https" =>
            postHttp(uriURI, session, entity)
          case "ftp" =>
            uploadFtp(uriURI, session, entity)
          case _ =>
            writeHDFS(uriURI, session, entity, overwrite = false)
        }
        Seq(result)
    }
  }

  def postHttp(uri: URI, session: Session, entity: HttpEntity): Fetched = {

    val (httpClient: CloseableHttpClient, context: HttpClientContext) = getHttpClient(session)

    val userAgent = session.spooky.spookyConf.userAgentFactory()
    val headers = session.spooky.spookyConf.headersFactory()

    val request = {
      val request = new HttpPost(uri)
      if (userAgent != null) request.addHeader("User-Agent", userAgent)
      for (pair <- headers) {
        request.addHeader(pair._1, pair._2)
      }
      request.setEntity(entity)

      request
    }

    httpInvoke(httpClient, context, request, cacheLevel = CacheLevel.NoCache)
  }

  def writeHDFS(uri: URI, session: Session, entity: HttpEntity, overwrite: Boolean = true): Fetched = {
    val path = new Path(uri.toString)

    val result: Fetched = this.writeHDFSFile(path, session, entity, overwrite)
    result
  }

  //not cacheable
  def writeHDFSFile(path: Path, session: Session, entity: HttpEntity, overwrite: Boolean = true): Fetched = {
    val size = HDFSResolver(session.spooky.hadoopConf)
      .output(path.toString, overwrite) {
        fos =>
          IOUtils.copyLarge(entity.getContent, fos) //Overkill?
      }

    val result = NoDoc(
      List(this),
      cacheLevel = CacheLevel.NoCache,
      metadata = Map("byteUploaded" -> size)
    )
    result
  }

  def uploadFtp(uri: URI, session: Session, entity: HttpEntity): Fetched = {

    val uc: URLConnection = getURLConn(uri, session)
    val stream = uc.getOutputStream

    val length = try {
      IOUtils.copyLarge(entity.getContent, stream) //Overkill?
    }
    finally {
      stream.close()
    }

    val result = NoDoc(
      List(this),
      cacheLevel = CacheLevel.NoCache
    )

    result
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val uriLit: Option[Lit[FR, String]] = resolveURI(pageRow, schema)

    uriLit.flatMap(
      lit =>
        this.copy(uri = lit)(entity).asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
    )
  }
}