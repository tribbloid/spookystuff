package com.tribbloids.spookystuff.web.conf

// Required dependency:
// implementation("org.seleniumhq.selenium:selenium-java:4.9.1")

import org.openqa.selenium.chrome.{ChromeDriver, ChromeDriverService, ChromeOptions}
import org.scalatest.funspec.AnyFunSpec

class SeleniumAutoDriverSpike extends AnyFunSpec {

  import SeleniumAutoDriverSpike.*

  it("main") {
    // This will automatically download the matching chromedriver binary
    // using Selenium 4.9.1's built-in driver management
    getAndClean(v => v.get("https://www.selenium.dev/documentation/selenium_manager/"))
  }

  ignore("resource cleanup stress test, memory consumption should be static") {

    for (_ <- 1 to 10000000) {

      getAndClean(_ => {})
    }
  }
}

object SeleniumAutoDriverSpike {

  def getAndClean[T](
      fn: ChromeDriver => T
  ): Unit = {
    val service = ChromeDriverService.createDefaultService

    val options = new ChromeOptions()
    options.addArguments("--headless=new") // Use headless mode

    val driver: ChromeDriver = new ChromeDriver(service, options)

    fn(driver)

    stopDriver(driver)
  }

  private def stopDriver(driver: ChromeDriver): Unit = {
    driver.quit()
  }
}
