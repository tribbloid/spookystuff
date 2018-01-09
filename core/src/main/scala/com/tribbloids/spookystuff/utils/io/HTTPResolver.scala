package com.tribbloids.spookystuff.utils.io

import java.io._
import java.net.{InetSocketAddress, URI}
import javax.net.ssl.SSLContext

import com.tribbloids.spookystuff.session.WebProxySetting
import com.tribbloids.spookystuff.utils.http._
import org.apache.http.client.HttpClient
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.protocol.HttpCoreContext
import org.apache.http.{HttpEntity, HttpHost}
import org.apache.spark.ml.dsl.utils.metadata.MetadataMap

object HTTPResolver {

  def getHttpContext(proxy: WebProxySetting): HttpClientContext = {

    val context: HttpClientContext = HttpClientContext.create

    if (proxy != null && proxy.protocol.startsWith("socks")) {
      val socksaddr: InetSocketAddress = new InetSocketAddress(proxy.addr, proxy.port)
      context.setAttribute("socks.address", socksaddr)

      context
    }
    context
  }

  def apply(
             timeoutMillis: Int,
             webProxy: WebProxySetting,
             input2Http: URI => HttpRequestBase
           ): HTTPResolver = {

    val (httpClient: CloseableHttpClient, httpClientContext: HttpClientContext) = {

      val requestConfig = {

        var builder = RequestConfig.custom()
          .setConnectTimeout(timeoutMillis)
          .setConnectionRequestTimeout(timeoutMillis)
          .setSocketTimeout(timeoutMillis)
          .setRedirectsEnabled(true)
          .setCircularRedirectsAllowed(true)
          .setRelativeRedirectsAllowed(true)
          .setAuthenticationEnabled(false)
        //        .setCookieSpec(CookieSpecs.BEST_MATCH)

        if (webProxy != null && !webProxy.protocol.startsWith("socks")) builder = builder.setProxy(new HttpHost(webProxy.addr, webProxy.port, webProxy.protocol))

        val result = builder.build()
        result
      }

      val sslContext: SSLContext = SSLContext.getInstance("SSL")
      sslContext.init(null, Array(new InsecureTrustManager()), null)
      val hostVerifier = new InsecureHostnameVerifier()

      val httpClient = if (webProxy != null && webProxy.protocol.startsWith("socks")) {
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

      val context: HttpClientContext = getHttpContext(webProxy)
      (httpClient, context)
    }

    HTTPResolver(httpClient, httpClientContext, input2Http)
  }
}

case class HTTPResolver(
                         client: HttpClient,
                         context: HttpClientContext,
                         //                         headers: Map[String, String] = Map.empty,
                         input2Request: URI => HttpRequestBase = {
                           v =>
                             new HttpGet(v)
                         }
                         //TODO: add back
                         //                         output2Request: URI => HttpEntityEnclosingRequestBase = {
                         //                           v =>
                         //                             new HttpPost(v)
                         //                         }
                       ) extends URIResolver {

  override def input[T](pathStr: String)(f: InputStream => T) = {

    val uri = HttpUtils.uri(pathStr)
    val request = input2Request(uri)
    httpInvoke(request)(f)
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T) = {
    ???
  }

  def httpInvoke[T](
                     request: HttpUriRequest
                   )(
                     processResponseBody: InputStream => T
                   ): Resource[T] = {
    val response = client.execute(request, context)
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

      val is = entity.getContent
      val result: T = try {
        processResponseBody(is)
      }
      finally {
        is.close()
      }

      SimpleResource(
        result,
        {
          import Resource._

          val map = MetadataMap(
            LENGTH -> entity.getContentLength
          )
          val headers: Seq[(String, String)] = response.getAllHeaders.map {
            header =>
              header.getName -> header.getValue
          }
            .toSeq

          map ++ headers ++ MetadataMap(
            URI_ -> request.getURI.toString,
            NAME -> entity.getContentType.getName
          )
        }
      )
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
}
