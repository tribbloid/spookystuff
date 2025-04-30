package com.tribbloids.spookystuff.web.agent

import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.WebDriver

case class WebDriverBundle(
    service: DriverService,
    driver: WebDriver
) {
  // selenium has a strict 1-on-1 mapping between service and driver
  // they should always be bundled into a single unit

}
