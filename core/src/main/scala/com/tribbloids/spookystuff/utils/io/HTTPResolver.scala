package com.tribbloids.spookystuff.utils.io

import java.io.{Closeable, InputStream, OutputStream}
import java.net.URI

import com.tribbloids.spookystuff.utils.http.HttpUtils
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpUriRequest}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.protocol.HttpCoreContext
import org.apache.http.{HttpEntity, HttpHost}

class HTTPResolver(
                    client: HttpClient,
                    context: HttpClientContext,
                    headers: Map[String, String] = Map.empty,
                    inputFactory: URI => HttpUriRequest = {
                      v =>
                        new HttpGet(v)
                    }
                  ) extends URIResolver {

  override def input[T](pathStr: String)(f: InputStream => T) = {

    val uri = HttpUtils.uri(pathStr)
    val request = inputFactory(uri)
    for (pair <- headers) {
      request.addHeader(pair._1, pair._2)
    }
    httpInvoke(request)(f)
  }

  override def output[T](pathStr: String, overwrite: Boolean)(f: OutputStream => T) = ???

  def httpInvoke[T](
                     request: HttpUriRequest
                   )(fn: InputStream => T): Resource[T] = {
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

      val stream = entity.getContent

      val result = try {
        fn(stream)
      }
      finally {
        stream.close()
      }

      new Resource(result) {
        override val metadata = {
          val map = Map(
            "length" -> entity.getContentLength
          )

          ResourceMetadata(
            uri = request.getURI.toString,
            name = Some(entity.getContentType.getName),
            declaredContentType = Some(entity.getContentType.getValue),
            misc = map
          )
        }

        val httpStatus = response.getStatusLine
        val locale = response.getLocale
        val headers = response.getAllHeaders
      }
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
