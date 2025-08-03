val vs = versions()

//plugins {
//    id("com.gradleup.shadow")
//}

dependencies {

    val tikaV = "3.2.1"

    api("org.apache.tika:tika-core:${tikaV}")
//    testImplementation( "org.apache.tika:tika-parsers-standard-package:${tikaV}")
    api("org.apache.tika:tika-parsers-standard-package:${tikaV}")

//    api("org.apache.tika:tika-parsers-pdf-module:${tikaV}")
}

tasks {
    shadowJar {

        relocate("org.apache.commons.io", "repacked.tika.org.apache.commons.io")
        relocate("org.apache.logging.log4j", "repacked.tika.org.apache.logging.log4j")
        relocate("com.fasterxml.jackson", "repacked.tika.com.fasterxml.jackson")
    }
}
