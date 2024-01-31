package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.agent.WebProxySetting
import com.tribbloids.spookystuff.utils.Retry
import com.tribbloids.spookystuff.utils.http._
import org.apache.hadoop.shaded.org.apache.http.client.HttpClient
import org.apache.hadoop.shaded.org.apache.http.client.config.RequestConfig
import org.apache.hadoop.shaded.org.apache.http.client.methods.{HttpGet, HttpRequestBase, HttpUriRequest}
import org.apache.hadoop.shaded.org.apache.http.client.protocol.HttpClientContext
import org.apache.hadoop.shaded.org.apache.http.config.RegistryBuilder
import org.apache.hadoop.shaded.org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.hadoop.shaded.org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.hadoop.shaded.org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.hadoop.shaded.org.apache.http.{HttpEntity, HttpHost, HttpResponse}
import org.apache.spark.ml.dsl.utils.LazyVar

import java.io._
import java.net.{InetSocketAddress, URI}
import javax.net.ssl.SSLContext

object HTTPResolver {

  def getHttpContext(proxy: WebProxySetting): HttpClientContext = {

    val context: HttpClientContext = HttpClientContext.create

    if (proxy != null && proxy.protocol.startsWith("socks")) {
      val socksAddr: InetSocketAddress = new InetSocketAddress(proxy.addr, proxy.port)
      context.setAttribute("socks.address", socksAddr)

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

        var builder = RequestConfig
          .custom()
          .setConnectTimeout(timeoutMillis)
          .setConnectionRequestTimeout(timeoutMillis)
          .setSocketTimeout(timeoutMillis)
          .setRedirectsEnabled(true)
          .setCircularRedirectsAllowed(true)
          .setRelativeRedirectsAllowed(true)
          .setAuthenticationEnabled(false)
        //        .setCookieSpec(CookieSpecs.BEST_MATCH)

        if (webProxy != null && !webProxy.protocol.startsWith("socks"))
          builder = builder.setProxy(new HttpHost(webProxy.addr, webProxy.port, webProxy.protocol))

        val result = builder.build()
        result
      }

      val sslContext: SSLContext = SSLContext.getInstance("SSL")
      sslContext.init(null, Array(new InsecureTrustManager()), null)
      val hostVerifier = new InsecureHostnameVerifier()

      val httpClient = if (webProxy != null && webProxy.protocol.startsWith("socks")) {
        val reg = RegistryBuilder
          .create[ConnectionSocketFactory]
          .register("http", new SocksProxyConnectionSocketFactory())
          .register("https", new SocksProxySSLConnectionSocketFactory(sslContext))
          .build()
        val cm = new PoolingHttpClientConnectionManager(reg)

        val httpClient = HttpClients.custom
          .setConnectionManager(cm)
          .setDefaultRequestConfig(requestConfig)
          .setRedirectStrategy(new ResilientRedirectStrategy())
          .setSSLContext(sslContext) // WARNING: keep until Spark get rid of httpclient 4.3
          .setSSLHostnameVerifier(hostVerifier) // WARNING: keep until Spark get rid of httpclient 4.3
          .build

        httpClient
      } else {
        val httpClient = HttpClients.custom
          .setDefaultRequestConfig(requestConfig)
          .setRedirectStrategy(new ResilientRedirectStrategy())
          .setSSLContext(sslContext) // WARNING: keep until Spark get rid of httpclient 4.3
          .setSSLHostnameVerifier(hostVerifier) // WARNING: keep until Spark get rid of httpclient 4.3
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
    readRequestFactory: URI => HttpRequestBase = { v =>
      new HttpGet(v)
    },
    //                         output2Request: URI => HttpEntityEnclosingRequestBase = { //TODO: need test
    //                           v =>
    //                             new HttpPost(v)
    //                         }
    override val retry: Retry = Retry.ExponentialBackoff(8, 16000)
) extends URIResolver {

//  val currentReq = context.getAttribute(HttpCoreContext.HTTP_REQUEST).asInstanceOf[HttpUriRequest]
//  val currentHost = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST).asInstanceOf[HttpHost]
//  val currentUrl = if (currentReq.getURI.isAbsolute) {
//    currentReq.getURI.toString
//  }
//  else {
//    currentHost.toURI + currentReq.getURI
//  }

  case class _Execution(pathStr: String) extends Execution {

    lazy val readRequest: HttpUriRequest = {

      val uri = HttpUtils.uri(pathStr)
      readRequestFactory(uri)
    }

    override def absolutePathStr: String = readRequest.getURI.toString

    override def _delete(mustExist: Boolean): Unit = {
      unsupported("delete")
    }

    override def moveTo(target: String, force: Boolean = false): Unit =
      unsupported("move")

    case class _Resource(mode: WriteMode) extends Resource with MimeTypeMixin {

      override protected def _outer: URIExecution = _Execution.this

      lazy val _readResponse: LazyVar[HttpResponse] = LazyVar {
        client.execute(readRequest, context)
      }

      lazy val entity: HttpEntity = _readResponse.value.getEntity

      override lazy val getName: String = entity.getContentType.getName

      override lazy val getContentType: String = entity.getContentType.getValue

      override lazy val getLength: Long = entity.getContentLength

      override lazy val getStatusCode: Option[Int] = Some(_readResponse.getStatusLine.getStatusCode)

      override lazy val getLastModified: Long = -1

      override lazy val _metadata: ResourceMetadata = {
        val mapped = _readResponse.getAllHeaders.map { header =>
          header.getName -> header.getValue
        }.toSeq
        ResourceMetadata.From.tuple(mapped: _*)
      }

      override def cleanImpl(): Unit = {
        super.cleanImpl()
        _readResponse.peek.foreach {
          case v: Closeable => v.close()
          case _            =>
        }
      }

      override protected def _newIStream: InputStream = {

        entity.getContent
      }

      override protected def _newOStream: OutputStream = {

        unsupported("write")
      }

    }
  }
}
