val vs = versions()

//plugins {
//    id("com.gradleup.shadow")
//}

dependencies {

//    constraints { // TODO: doesn't work, overriding is broken
//
//        runtimeOnly("${vs.scala.group}:scala-library:${vs.scala.v}")
//        runtimeOnly("${vs.scala.group}:scala-reflect:${vs.scala.v}")
//    }

    val seleniumV = "4.1.4"
    val phantomJSV = "1.5.0"
    val htmlUnitV = "3.61.0"

    // Selenium
    api("org.seleniumhq.selenium:selenium-api:${seleniumV}")
    api("org.seleniumhq.selenium:selenium-support:${seleniumV}")

//    "org.seleniumhq.selenium:selenium-firefox-driver" % seleniumV
//    "org.seleniumhq.selenium:selenium-ie-driver" % seleniumV

    api("com.codeborne:phantomjsdriver:${phantomJSV}")

    api("org.seleniumhq.selenium:htmlunit-driver:${htmlUnitV}")
//    api("org.seleniumhq.selenium:selenium-support-htmlunit:${seleniumV}")

//    "org.seleniumhq.selenium:selenium-support-async" % seleniumV
//    "org.seleniumhq.selenium:selenium-support-events" % seleniumV
//    "org.seleniumhq.selenium:selenium-support-ui" % seleniumV

//    "org.seleniumhq.selenium:selenium-support-webdriver" % seleniumV
//    "org.seleniumhq.selenium:selenium-support-webdriver-firefox" % seleniumV
//    "org.seleniumhq.selenium:selenium-support-webdriver-ie" % seleniumV
}

tasks {
    shadowJar {

        relocate("com.google.common", "repacked.selenium.com.google.common")
        relocate("io.netty", "repacked.selenium.io.netty")
    }
}
