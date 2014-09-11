package org.tribbloid.spookystuff.entity.clientaction

import java.net._
import javax.net.ssl.{TrustManager, SSLContext, HttpsURLConnection}

import org.apache.commons.io.IOUtils
import org.apache.http.conn.ssl.AllowAllHostnameVerifier
import org.tribbloid.spookystuff.Const
import org.tribbloid.spookystuff.entity.Page
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.utils.InsecureTrustManager

/**
 * Export a page from the browser or http client
 * the page an be anything including HTML/XML file, image, PDF file or JSON string.
 */
trait Export extends ClientAction {
  var alias: String = null

  def as(alias: String): this.type = { //TODO: better way to return type?
    this.alias = alias
    this
  }
}

/**
 * Export the current page from the browser
 * interact with the browser to load the target page first
 * only for html page, please use wget for images and pdf files
 * always export as UTF8 charset
 */
case class Snapshot() extends Export {
  // all other fields are empty
  override def doExe(pb: PageBuilder): Array[Page] = {
    val page =       new Page(
      pb.driver.getCurrentUrl,
      pb.driver.getPageSource.getBytes("UTF8"),
      contentType = "text/html; charset=UTF-8"
    )

    val backtrace = pb.backtrace.toArray(new Array[Interactive](pb.backtrace.size()))

    Array[Page](page.copy(backtrace = backtrace))
  }
}

/**
 * use an http GET to fetch a remote resource deonted by url
 * http client is much faster than browser, also load much less resources
 * recommended for most static pages.
 * actions for more complex http/restful API call will be added per request.
 * @param url support cell interpolation
 */
case class Wget(url: String) extends Export with Sessionless{

  override def exeWithoutSession: Array[Page] = {
    if ((url == null)|| url.isEmpty) return Array[Page]()

    CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL))

    val uc: URLConnection =  new URL(url).openConnection()
    uc.setConnectTimeout(Const.resourceTimeout*1000)
    uc.setReadTimeout(Const.resourceTimeout*1000)

    uc match {
      case huc: HttpsURLConnection =>
        // Install the all-trusting trust manager
        val sslContext = SSLContext.getInstance( "SSL" )
        sslContext.init(null, Array[TrustManager](new InsecureTrustManager()), null)
        // Create an ssl socket factory with our all-trusting manager
        val sslSocketFactory  = sslContext.getSocketFactory

        huc.setSSLSocketFactory(sslSocketFactory)
        huc.setHostnameVerifier(new AllowAllHostnameVerifier)

      case _ =>
    }

    uc.connect()
    val is = uc.getInputStream

    val content = IOUtils.toByteArray(is)

    is.close()

    Array[Page](
      new Page(url,
        content,
        contentType = uc.getContentType
      ) //will not export backtrace right now
    )
  }

  override def interpolate[T](map: Map[String,T]): this.type = {
    Wget(ClientAction.interpolate(this.url,map)).asInstanceOf[this.type]
  }
}
