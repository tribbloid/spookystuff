package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.utils.CommonConst
import org.spark_project.jetty.server.handler.{DefaultHandler, HandlerList, HandlerWrapper, ResourceHandler}
import org.spark_project.jetty.server.{Request, Server}

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

object TestSiteServer {

  import com.tribbloids.spookystuff.utils.CommonViews._

  object RedirectHandler extends HandlerWrapper {

    override def handle(
        target: String,
        baseRequest: Request,
        request: HttpServletRequest,
        response: HttpServletResponse
    ): Unit = {

      val redirectURIOpt = rewrite(request.getRequestURI)
      redirectURIOpt.foreach { v =>
        response.sendRedirect(v)
        baseRequest.setHandled(true)
      //https://stackoverflow.com/questions/72410379/strange-jetty-warning-java-lang-illegalstateexception-committed-on-redirectin
      }
    }

    def rewrite(target: String): Option[String] = {
      if (target.endsWith(".html")) None
      else {
        var _target = target.stripSuffix("/")
        if (_target.nonEmpty) {
          _target += ".html"
          Some(_target)
        } else {
          None
        }
      }
    }
  }

  lazy val server: Server = {
    val server = new Server(10092)

    val resourceHandler = new ResourceHandler

    resourceHandler.setDirectoriesListed(true)
    resourceHandler.setWelcomeFiles(Array[String]("test-sites.html"))
    resourceHandler.setResourceBase(CommonConst.USER_DIR \\ "test-sites")

    val handlers = new HandlerList
    handlers.setHandlers(Array(RedirectHandler, resourceHandler, new DefaultHandler))
    server.setHandler(handlers)
    server
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    server.start()
    server.join()
  }
}
