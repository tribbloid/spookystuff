package org.tribbloid.spookystuff.actions

import java.net.InetSocketAddress

import org.apache.commons.io.IOUtils
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.ssl.SSLContexts
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.openqa.selenium.{OutputType, TakesScreenshot}
import org.tribbloid.spookystuff.entity.{Page, PageRow, PageUID}
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.utils.{Const, SocksProxyConnectionSocketFactory, SocksProxySSLConnectionSocketFactory, Utils}

/**
 * Export a page from the browser or http client
 * the page an be anything including HTML/XML file, image, PDF file or JSON string.
 */
abstract class Export extends Action {
  var alias: String = null

  def as(alias: String): this.type = {
    this.alias = alias
    this
  }

  final override def mayExport() = true

  final override def trunk() = None //have not impact to driver

  def doExe(session: PageBuilder) = doExeNoAlias(session).map(_.copy(name = this.alias))

  def doExeNoAlias(session: PageBuilder): Seq[Page]

  //  def doExe(pb: PageBuilder): Seq[Page]
}

/**
 * Export the current page from the browser
 * interact with the browser to load the target page first
 * only for html page, please use wget for images and pdf files
 * always export as UTF8 charset
 */
case class Snapshot() extends Export {

  // all other fields are empty
  override def doExeNoAlias(pb: PageBuilder): Seq[Page] = {

    //    import scala.collection.JavaConversions._

    //    val cookies = pb.driver.manage().getCookies
    //    val serializableCookies = ArrayBuffer[SerializableCookie]()
    //
    //    for (cookie <- cookies) {
    //      serializableCookies += cookie.asInstanceOf[SerializableCookie]
    //    }

    val page = Page(
      PageUID(pb.realBacktrace :+ this),
      pb.existingDriver.get.getCurrentUrl,
      "text/html; charset=UTF-8",
      pb.existingDriver.get.getPageSource.getBytes("UTF8")
      //      serializableCookies
    )

    Seq(page)
  }
}

//this is used to save GC when invoked by anothor component
object DefaultSnapshot extends Snapshot()

case class Screenshot() extends Export {

  override def doExeNoAlias(pb: PageBuilder): Seq[Page] = {

    val content = pb.existingDriver.get match {
      case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
      case _ => throw new UnsupportedOperationException("driver doesn't support snapshot")
    }

    val page = Page(
      PageUID(pb.realBacktrace :+ this),
      pb.existingDriver.get.getCurrentUrl,
      "image/png",
      content
    )

    Seq(page)
  }
}

object DefaultScreenshot extends Screenshot()

/**
 * use an http GET to fetch a remote resource deonted by url
 * http client is much faster than browser, also load much less resources
 * recommended for most static pages.
 * actions for more complex http/restful API call will be added per request.
 * @param url support cell interpolation
 */
case class Wget(
                 url: String,
                 userAgent: String = Const.userAgent,
                 headers: Map[String, String] = Map()
                 ) extends Export with Sessionless {

  override def doExeNoAlias(pb: PageBuilder): Seq[Page] = {

    if ( url.trim().isEmpty ) return Seq ()

    val proxy = pb.spooky.proxy

    val defaultSetting = {
      val timeoutMillis = pb.spooky.remoteResourceTimeout.toMillis.toInt

      var builder = RequestConfig.custom()
        .setConnectTimeout ( timeoutMillis )
        .setConnectionRequestTimeout ( timeoutMillis )
        .setSocketTimeout( timeoutMillis )

      if (proxy!=null && !proxy.protocol.startsWith("socks")) builder=builder.setProxy(new HttpHost(proxy.addr, proxy.port, proxy.protocol))

      val settings = builder.build()
      settings
    }

    val httpClient = if (proxy !=null && proxy.protocol.startsWith("socks")) {
      val reg = RegistryBuilder.create[ConnectionSocketFactory]
        .register("http", new SocksProxyConnectionSocketFactory())
        .register("https", new SocksProxySSLConnectionSocketFactory(SSLContexts.createSystemDefault()))
        .build()
      val cm = new PoolingHttpClientConnectionManager(reg)

      val httpClient = HttpClients.custom
        .setDefaultRequestConfig ( defaultSetting )
        .setConnectionManager(cm)
        .build

      httpClient
    }
    else {
      val httpClient = HttpClients.custom
        .setDefaultRequestConfig ( defaultSetting )
        .build()

      httpClient
    }

    val request = {
      val request = new HttpGet(url)
      if (userAgent != null) request.addHeader("User-Agent", userAgent)
      for (pair <- headers) {
        request.addHeader(pair._1, pair._2)
      }

      request
    }
    val context = if (proxy !=null && proxy.protocol.startsWith("socks")) {
      val socksaddr: InetSocketAddress = new InetSocketAddress(proxy.addr, proxy.port)
      val context: HttpClientContext = HttpClientContext.create
      context.setAttribute("socks.address", socksaddr)

      context
    }
    else null

    val response = httpClient.execute ( request, context )
    try {
      //      val httpStatus = response.getStatusLine().getStatusCode()
      val entity = response.getEntity

      val stream = entity.getContent
      try {
        val content = IOUtils.toByteArray ( stream )
        val contentType = entity.getContentType.getValue

        val result = Page(
          PageUID(pb.realBacktrace :+ this),
          url,
          contentType,
          content
        )

        assert(!result.contentStr.contains("<title></title>"))

        Seq(result)
      }
      finally {
        stream.close()
      }
    }
    finally {
      response.close()
    }
  }

  override def interpolate(pageRow: PageRow): this.type = {
    //ugly workaround of https://issues.scala-lang.org/browse/SI-7005
    val interpolatedHeaders = this.headers.mapValues(value => Utils.interpolateFromMap(value, pageRow.cells)).map(identity)

    this.copy(
      url = Utils.interpolateFromMap(this.url,pageRow.cells),
      userAgent = Utils.interpolateFromMap(this.userAgent, pageRow.cells),
      headers = interpolatedHeaders
    ).asInstanceOf[this.type]
  }
}