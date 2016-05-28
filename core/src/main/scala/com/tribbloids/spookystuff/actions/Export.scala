package com.tribbloids.spookystuff.actions

import java.lang.reflect.InvocationTargetException
import java.net.{InetSocketAddress, URI}
import java.util.Date
import javax.net.ssl.SSLContext

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.execution.SchemaContext
import com.tribbloids.spookystuff.extractors.{Extractor, Literal}
import com.tribbloids.spookystuff.http._
import com.tribbloids.spookystuff.row.FetchedRow
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.{HDFSResolver, Utils}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
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

import scala.xml._

/**
  * Export a page from the browser or http client
  * the page an be anything including HTML/XML file, image, PDF file or JSON string.
  */
trait Export extends Named with Wayback{

  def filter: DocFilter

  final override def outputNames = Set(this.name)

  final override def trunk = None //have not impact to driver

  protected final def doExe(session: Session): Seq[Fetched] = {
    val results = doExeNoName(session)
    results.map{
      case page: Doc =>
        try {
          filter.apply(page, session)
        }
        catch {
          case e: Throwable =>
            var message = "\n\n+>" + this.toString
            message += "\n" +e.getMessage
            val errorDump = session.spooky.conf.errorDump

            if (errorDump) {
              message += "\nSnapshot: " +this.errorDump(message, page, session.spooky)
            }

            throw new DocFilterError(page, message, e)
        }
      case other: Fetched =>
        other
    }
  }

  protected def doExeNoName(session: Session): Seq[Fetched]
}

trait WaybackSupport {
  self: Wayback =>

  var wayback: Extractor[Long] = null

  def waybackTo(date: Extractor[Date]): this.type = {
    this.wayback = date.andFn(_.getTime)
    this
  }

  def waybackTo(date: Date): this.type = this.waybackTo(Literal(date))

  def waybackToTimeMillis(time: Extractor[Long]): this.type = {
    this.wayback = time
    this
  }

  def waybackToTimeMillis(date: Long): this.type = this.waybackToTimeMillis(Literal(date))

  protected def interpolateWayback(pageRow: FetchedRow, schema: SchemaContext): Option[this.type] = {
    if (this.wayback == null) Some(this)
    else {
      val valueOpt = this.wayback.resolve(schema).lift(pageRow)
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
                     override val filter: DocFilter = Const.defaultDocumentFilter
                   ) extends Export with WaybackSupport{

  // all other fields are empty
  override def doExeNoName(pb: Session): Seq[Doc] = {

    //    import scala.collection.JavaConversions._

    //    val cookies = pb.driver.manage().getCookies
    //    val serializableCookies = ArrayBuffer[SerializableCookie]()
    //
    //    for (cookie <- cookies) {
    //      serializableCookies += cookie.asInstanceOf[SerializableCookie]
    //    }

    val page = new Doc(
      DocUID((pb.backtrace :+ this).toList, this),
      pb.driver.getCurrentUrl,
      Some("text/html; charset=UTF-8"),
      pb.driver.getPageSource.getBytes("UTF8")
      //      serializableCookies
    )

    //    if (contentType != null) Seq(page.copy(declaredContentType = Some(contentType)))
    Seq(page)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SchemaContext) = {
    this.copy().asInstanceOf[this.type].interpolateWayback(pageRow, schema)
  }
}

//this is used to save GC when invoked by anothor component
object DefaultSnapshot extends Snapshot()

case class Screenshot(
                       override val filter: DocFilter = Const.defaultImageFilter
                     ) extends Export with WaybackSupport {

  override def doExeNoName(pb: Session): Seq[Doc] = {

    val content = pb.driver match {
      case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
      case _ => throw new UnsupportedOperationException("driver doesn't support snapshot")
    }

    val page = new Doc(
      DocUID((pb.backtrace :+ this).toList, this),
      pb.driver.getCurrentUrl,
      Some("image/png"),
      content
    )

    Seq(page)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SchemaContext) = {
    this.copy().asInstanceOf[this.type].interpolateWayback(pageRow, schema)
  }
}

object DefaultScreenshot extends Screenshot()

/**
  * use an http GET to fetch a remote resource deonted by url
  * http client is much faster than browser, also load much less resources
  * recommended for most static pages.
  * actions for more complex http/restful API call will be added per request.
  *
  * @param uri support cell interpolation
  */
case class Wget(
                 uri: Extractor[Any],
                 override val filter: DocFilter = Const.defaultDocumentFilter
               ) extends Export with Driverless with Timed with WaybackSupport {

  lazy val uriOption: Option[URI] = {
    val uriStr = uri.asInstanceOf[Literal[String]].value.trim()
    if ( uriStr.isEmpty ) None
    else Some(HttpUtils.uri(uriStr))
  }

  //  def effectiveURIString = uriOption.map(_.toString)

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
            getHDFS(uriURI, session)
        }
        //        if (this.contentType != null) result.map{
        //          case page: Page => page.copy(declaredContentType = Some(this.contentType))
        //          case others: Fetched => others
        //        }
        result
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
  //    Seq(result)
  //  }

  def getHDFS(uri: URI, session: Session): Seq[Fetched] = {
    val spooky = session.spooky
    val path = new Path(uri.toString)

    val fs = path.getFileSystem(spooky.hadoopConf)

    if (fs.exists(path)) {
      val result: Seq[Fetched] = if (fs.getFileStatus(path).isDirectory) {
        this.getHDFSDirectory(path, fs)
      }
      else {
        this.getHDFSFile(path, session)
      }
      result
    }
    else
      Seq(NoDoc(List(this), cacheable = false))
  }

  def getHDFSDirectory(path: Path, fs: FileSystem): Seq[Fetched] = {
    val statuses = fs.listStatus(path)
    val xmls: Array[Elem] = statuses.map {
      (status: FileStatus) =>
        //use reflection for all getter & boolean getter
        //TODO: move to utility
        val methods = status.getClass.getMethods
        val getters = methods.filter {
          m =>
            m.getName.startsWith("get") && (m.getParameterCount == 0)
        }
          .map(v => v.getName.stripPrefix("get") -> v)
        val booleanGetters = methods.filter {
          m =>
            m.getName.startsWith("is") && (m.getParameterCount == 0)
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
    val xmlStr = Utils.xmlPrinter.format(xml)

    val result: Seq[Fetched] = Seq(new Doc(
      DocUID(List(this), this),
      path.toString,
      Some("inode/directory; charset=UTF-8"),
      xmlStr.getBytes("utf-8"),
      cacheable = false
    ))
    result
  }

  //not cacheable
  def getHDFSFile(path: Path, session: Session): Seq[Fetched] = {
    val content =
      HDFSResolver(session.spooky.hadoopConf).input(path.toString) {
        fis =>
          IOUtils.toByteArray(fis)
      }

    val result = new Doc(
      DocUID(List(this), this),
      path.toString,
      None,
      content,
      cacheable = false
    )

    Seq(result)
  }

  def getFtp(uri: URI, session: Session): Seq[Fetched] = {

    val timeoutMs = this.timeout(session).toMillis.toInt

    val uc = uri.toURL.openConnection()
    uc.setConnectTimeout(timeoutMs)
    uc.setReadTimeout(timeoutMs)

    uc.connect()
    uc.getInputStream
    val stream = uc.getInputStream

    val content = IOUtils.toByteArray ( stream )

    val result = new Doc(
      DocUID(List(this), this),
      uri.toString,
      None,
      content
    )

    Seq(result)
  }

  def getHttp(uri: URI, session: Session): Seq[Fetched] = {

    val proxy = session.spooky.conf.proxy()
    val userAgent = session.spooky.conf.userAgentFactory()
    val headers = session.spooky.conf.headersFactory()
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
        val httpStatus: StatusLine = response.getStatusLine

        val stream = entity.getContent
        val result = try {
          val content = IOUtils.toByteArray ( stream )
          val contentType = entity.getContentType.getValue

          new Doc(
            DocUID(List(this), this),
            currentUrl,
            Some(contentType),
            content,
            httpStatus = Some(httpStatus)
          )
        }
        finally {
          stream.close()
        }

        Seq(result)
      }
      finally {
        response.close()
      }
    }
    catch {
      case e: ClientProtocolException =>
        val cause = e.getCause
        if (cause.isInstanceOf[RedirectException]) Seq(NoDoc((session.backtrace :+ this).toList)) //TODO: is it a reasonable exception?
        else throw e
      case e: Throwable =>
        throw e
    }
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SchemaContext): Option[this.type] = {
    val first = this.uri.resolve(schema).lift(pageRow).flatMap(Utils.asArray[Any](_).headOption)

    val uriStr: Option[String] = first.flatMap {
      case element: Unstructured => element.href
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case other => None
    }

    uriStr.flatMap(
      str =>
        this.copy(uri = Literal(str)).interpolateWayback(pageRow, schema).map(_.asInstanceOf[this.type])
    )
  }
}