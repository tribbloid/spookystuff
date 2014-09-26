package org.tribbloid.spookystuff.entity.client

import java.net._
import javax.net.ssl.{HttpsURLConnection, SSLContext, TrustManager}

import org.apache.commons.io.IOUtils
import org.apache.http.conn.ssl.AllowAllHostnameVerifier
import org.openqa.selenium.{OutputType, TakesScreenshot}
import org.tribbloid.spookystuff.entity.{Page, PageUID}
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.utils.InsecureTrustManager

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

  def doExe(pb: PageBuilder): Seq[Page]
}

/**
 * Export the current page from the browser
 * interact with the browser to load the target page first
 * only for html page, please use wget for images and pdf files
 * always export as UTF8 charset
 */
case class Snapshot() extends Export {

  // all other fields are empty
  override def doExe(pb: PageBuilder): Seq[Page] = {

//    import scala.collection.JavaConversions._

//    val cookies = pb.driver.manage().getCookies
//    val serializableCookies = ArrayBuffer[SerializableCookie]()
//
//    for (cookie <- cookies) {
//      serializableCookies += cookie.asInstanceOf[SerializableCookie]
//    }

    val page = Page(
      PageUID(pb.backtrace :+ this),
      pb.driver.getCurrentUrl,
      "text/html; charset=UTF-8",
      pb.driver.getPageSource.getBytes("UTF8")
//      serializableCookies
    )

    Seq(page)
  }
}

//this is used to save GC when invoked by anothor component
object DefaultSnapshot extends Snapshot()

case class Screenshot() extends Export {

  override def doExe(pb: PageBuilder): Seq[Page] = {

    val content = pb.driver match {
      case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
      case _ => throw new UnsupportedOperationException("driver doesn't support snapshot")
    }

    val page = Page(
      PageUID(pb.backtrace :+ this),
      pb.driver.getCurrentUrl,
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
case class Wget(url: String) extends Export with Sessionless {

  override def doExe(pb: PageBuilder): Seq[Page] = {
    if ((url == null)|| url.isEmpty) return Array[Page]()

    CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL))

    val uc: URLConnection =  new URL(url).openConnection()
    uc.setConnectTimeout(pb.spooky.resourceTimeout.toMillis.toInt)
    uc.setReadTimeout(pb.spooky.resourceTimeout.toMillis.toInt)

    uc match {
      case huc: HttpsURLConnection =>
        // Install the all-trusting trust manager
        val sslContext = SSLContext.getInstance("SSL")
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

    val page = new Page(
      PageUID(pb.backtrace :+ this),
      url,
      "text/html; charset=UTF-8",
      content
    )

    Seq(page)
  }

  override def interpolateFromMap[T](map: Map[String,T]): this.type = {
    this.copy(url = Action.interpolateFromMap(this.url,map)).asInstanceOf[this.type]
  }
}