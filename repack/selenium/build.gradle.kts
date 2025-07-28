//val vs = versions()

dependencies {

    val seleniumV = "4.34.0"

    api("org.seleniumhq.selenium:selenium-api:${seleniumV}")

    api("org.seleniumhq.selenium:selenium-support:${seleniumV}") // TODO: why do I need this?

//    "org.seleniumhq.selenium:selenium-firefox-driver" % seleniumV
//    "org.seleniumhq.selenium:selenium-ie-driver" % seleniumV
    api("org.seleniumhq.selenium:selenium-chrome-driver:${seleniumV}")
//    api("org.seleniumhq.selenium:selenium-edge-driver:${seleniumV}")
    api("org.seleniumhq.selenium:selenium-firefox-driver:${seleniumV}")

    api("org.seleniumhq.selenium:selenium-manager:${seleniumV}")

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
