package com.tribbloids.spookystuff.integration

import java.io.File

import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.utils.CommonViews._
import fi.iki.elonen.SimpleWebServer

case class FileServer(
                       host: String,
                       port: Int = 10092,
                       wwwroot: File = new File(TestHelper.USER_DIR \\ "test-sites"),
                       quiet: Boolean = true,
                       cors: String = null
                     ) extends SimpleWebServer(host, port, wwwroot, quiet, cors) {


}

object FileServer {

  lazy val _server: FileServer = {
    FileServer(null)
  }

  def start(): Unit = {
    _server.start()
  }

  def stop(): Unit = {
    _server.stop()
  }

  def main(args: Array[String]): Unit = {
    start()
    Thread.sleep(Long.MaxValue)
  }
}