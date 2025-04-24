val vs = versions()

plugins {
    id("com.gradleup.shadow")
}

dependencies {

    val tikaV = "3.1.0"

    api("org.apache.tika:tika-core:${tikaV}")
//    testImplementation( "org.apache.tika:tika-parsers-standard-package:${tikaV}")
    api("org.apache.tika:tika-parsers-standard-package:${tikaV}")
}

tasks {
    shadowJar {

        setExcludes(

            listOf(
                "META-INF/*.SF",
                "META-INF/*.DSA",
                "META-INF/*.RSA",

                "**/scala/**"
            )
        )

        relocate("org.apache.commons.io", "repacked.spookystuff.org.apache.commons.io")
//        relocate("io.netty", "repacked.spookystuff.io.netty")
    }
}
