package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff.utils.CommonConst
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.spark_project.jetty.server.handler.{DefaultHandler, HandlerList, HandlerWrapper, ResourceHandler}
import org.spark_project.jetty.server.{Request, Server}

object TestSiteServer {

  import com.tribbloids.spookystuff.utils.CommonViews._

  class ExtHandler extends HandlerWrapper {

    override def handle(
                         target: String,
                         baseRequest: Request,
                         request: HttpServletRequest,
                         response: HttpServletResponse
                       ): Unit = {

      val redirectURIOpt = rewrite(request.getRequestURI)
      redirectURIOpt.foreach {
        v =>
          response.sendRedirect(v)
      }
    }

    def rewrite(target: String): Option[String] = {
      if (target.endsWith(".html")) None
      else {
        var _target = target.stripSuffix("/")
        if (_target.nonEmpty) {
          _target += ".html"
          Some(_target)
        }
        else {
          None
        }
      }
    }
  }

  lazy val server: Server = {
    val server = new Server(10092)

    val ext_handler = new ExtHandler
    val resource_handler = new ResourceHandler

    resource_handler.setDirectoriesListed(true)
    resource_handler.setWelcomeFiles(Array[String]("test-sites.html"))
    resource_handler.setResourceBase(CommonConst.USER_DIR \\ "test-sites")

    val handlers = new HandlerList
    handlers.setHandlers(Array(ext_handler, resource_handler, new DefaultHandler))
    server.setHandler(handlers)
    server
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    server.start()
    server.join()
  }
}
