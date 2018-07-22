package com.tribbloids.spookystuff.utils.io

import java.io._
import java.net.{InetSocketAddress, URI}

import com.tribbloids.spookystuff.session.WebProxySetting
import com.tribbloids.spookystuff.utils.http._
import javax.net.ssl.SSLContext
import org.apache.http.client.HttpClient
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.{HttpEntity, HttpHost, HttpResponse}

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
                         //                         output2Request: URI => HttpEntityEnclosingRequestBase = { //TODO: need test
                         //                           v =>
                         //                             new HttpPost(v)
                         //                         }
                       ) extends URIResolver {

//  val currentReq = context.getAttribute(HttpCoreContext.HTTP_REQUEST).asInstanceOf[HttpUriRequest]
//  val currentHost = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST).asInstanceOf[HttpHost]
//  val currentUrl = if (currentReq.getURI.isAbsolute) {
//    currentReq.getURI.toString
//  }
//  else {
//    currentHost.toURI + currentReq.getURI
//  }

  override def Execution(pathStr: String) = new Execution(pathStr)
  case class Execution(pathStr: String) extends super.Execution {

    override def absolutePathStr: String = pathStr

    override def input[T](f: InputResource => T) = {

      val uri = HttpUtils.uri(pathStr)
      val ir = new InputResource with HttpResource[InputStream] {
        override lazy val request: HttpUriRequest = input2Request(uri)

        override protected def _stream: InputStream = entity.getContent

        override lazy val isAlreadyExisting: Boolean = {
          getStatusCode.exists(_.toString.startsWith("2"))
        }
      }
      try {
        f(ir)
      }
      finally {
        ir.clean()
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

    override def _remove(mustExist: Boolean): Unit = {
      ???
    }

    override def output[T](overwrite: Boolean)(f: OutputResource => T): T = {
      ???
    }
  }

  trait HttpResource[T] extends Resource[T] {

    def request: HttpUriRequest

    @transient var existingResponse: HttpResponse = _
    lazy val response: HttpResponse = {
      existingResponse = client.execute(request, context)
      existingResponse
    }

    lazy val entity: HttpEntity = response.getEntity

    override lazy val getURI: String = request.getURI.toString

    override lazy val getName: String = entity.getContentType.getName

    override lazy val getContentType: String = entity.getContentType.getValue

    override lazy val getLenth: Long = entity.getContentLength

    override lazy val getStatusCode: Option[Int] = Some(response.getStatusLine.getStatusCode)

    override lazy val getLastModified: Long = -1

    override lazy val _metadata: ResourceMD = {
      val map = response.getAllHeaders.map {
        header =>
          header.getName -> header.getValue
      }
        .toSeq
      ResourceMD.apply(map: _*)
    }

    abstract override def cleanImpl(): Unit = {
      super.cleanImpl()
      Option(existingResponse).foreach {
        case v: Closeable => v.close()
        case _ =>
      }
    }
  }
}
