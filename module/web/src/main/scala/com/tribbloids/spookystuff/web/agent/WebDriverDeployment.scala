package com.tribbloids.spookystuff.web.agent

import java.net.URI
import java.nio.file.Path

case class WebDriverDeployment(
    bundle: WebDriverBundle,
    localSrc: Option[URI],
    target: Path
) {

  def deploy(): Unit = ???
}
