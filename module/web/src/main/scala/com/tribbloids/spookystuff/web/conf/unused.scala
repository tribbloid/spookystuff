package com.tribbloids.spookystuff.web.conf

object unused {

  //  case class HtmlUnit(
  //      browserV: BrowserVersion = BrowserVersion.getDefault
  //  ) extends WebDriverFactory {
  //
  //    override def _createImpl(agent: Agent, lifespan: Lifespan): CleanWebDriver = {
  //
  //      val caps = DesiredCapabilitiesView.default.Imported(agent.spooky).htmlUnit
  //
  //      val self = new HtmlUnitDriver(browserV)
  //      self.setJavascriptEnabled(true)
  //      self.setProxySettings(Proxy.extractFrom(caps))
  //      val result = new CleanWebDriver(self, _lifespan = lifespan)
  //
  //      result
  //    }
  //  }

  //  object PhantomJS {
  //
  //    object RepoURL {
  //
  //      final val linux_amd64 = "https://storage.googleapis.com/ci_public/phantom_JS/Linux_amd64/phantomjs"
  //      final val windows_amd64 = "https://storage.googleapis.com/ci_public/phantom_JS/MacOS/phantomjs"
  //      final val apple = "https://storage.googleapis.com/ci_public/phantom_JS/Windows_amd64/phantomjs.exe"
  //    }
  //
  //    @transient lazy val DEFAULT_PATH: String = {
  //      ConfUtils.getOrDefault(
  //        "phantomjs.path",
  //        Envs.USER_HOME \\ ".spookystuff" \\ "phantomjs"
  //      )
  //    }
  //
  //    def forceDelete(dst: String): Unit = this.synchronized {
  //      val dstFile = new File(dst)
  //      FileUtils.forceDelete(dstFile)
  //    }
  //
  ////    lazy val phantomJSBuilder: PhantomJSDriverService.Builder = {
  ////
  ////      new PhantomJSDriverService.Builder()
  ////        .usingCommandLineArguments(
  ////          Array(
  ////            "--webdriver-loglevel=WARN",
  ////            "--verbose",
  ////            "--log-path=fuckyou.log"
  ////          )
  ////        )
  //////        .usingCommandLineArguments(Array.empty)
  ////    }
  //  }

  //  case class PhantomJS(
  //      deploy: SpookyContext => BinaryDeployment = { _ =>
  //        BrowserDeployment()
  //      },
  //      loadImages: Boolean = false
  //  ) extends WebDriverFactory {
  //
  //    override def deployGlobally(spooky: SpookyContext): Unit = {
  //
  //      val deployment = deploy(spooky)
  //
  //      deployment.OnSparkDriver(spooky.sparkContext).addSparkFile
  //    }
  //
  //    case class DriverCreation(agent: Agent, lifespan: Lifespan) {
  //
  //      val deployment: BinaryDeployment = deploy(agent.spooky)
  //
  //      private val browserPath = deployment.deploydPath
  //
  //      val caps: DesiredCapabilities = {
  //
  //        val result = DesiredCapabilitiesView.default.Imported(agent.spooky).phantomJS
  //        result.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, browserPath)
  //        result
  //      }
  //
  ////      lazy val service: PhantomJSDriverService = {
  ////
  ////        val deployment = deploy(agent.spooky)
  ////
  ////        val proxyOpt = Option(agent.spooky.conf.webProxy(())).map { v =>
  ////          asSeleniumProxy(v)
  ////        }
  ////
  ////        import scala.jdk.CollectionConverters._
  ////
  ////        var builder = PhantomJS.phantomJSBuilder
  ////          .usingAnyFreePort()
  ////          .usingPhantomJSExecutable(new File(browserPath))
  ////          .withEnvironment(
  ////            Map(
  ////              "OPENSSL_CONF" -> "/dev/null" // https://github.com/bazelbuild/rules_closure/issues/351
  ////            ).asJava
  ////          )
  //////          .withLogFile(new File(s"${agent.Log.dirPath}/PhantomJSDriver.log"))
  ////          .withLogFile(new File("PhantomJSDriver.log"))
  ////          //        .withLogFile(new File("/dev/null"))
  ////          .usingGhostDriverCommandLineArguments(
  ////            Array(s"""service_log_path="${agent.Log.dirPath}/GhostDriver.log"""")
  ////          )
  ////        //        .usingGhostDriverCommandLineArguments(Array.empty)
  ////
  ////        proxyOpt.foreach { proxy =>
  ////          builder = builder.withProxy(proxy)
  ////        }
  ////
  ////        builder.build
  ////      }
  //
  //      //      val driver = new PhantomJSDriver(caps)
  //      lazy val webDriver: WebDriver = new PhantomJSDriver(service, caps)
  //
  //      lazy val cleanWebDriver: CleanWebDriver = {
  //
  //        val result = new CleanWebDriver(webDriver, Some(service), lifespan)
  //        result
  //      }
  //
  //    }
  //
  //    // called from executors
  //    override def _createImpl(agent: Agent, lifespan: Lifespan): CleanWebDriver = PhantomJS.synchronized {
  //      // synchronized to avoid 2 builders competing for the same port
  //      val creation = DriverCreation(agent, lifespan)
  //      creation.cleanWebDriver
  //    }
  //  }
}
