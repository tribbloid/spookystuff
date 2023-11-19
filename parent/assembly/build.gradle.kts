
val vs = versions()

//apply(plugin = "com.github.johnrengelman.shadow")
plugins {
    id("com.github.johnrengelman.shadow")
}

dependencies {

    api(project(":parent:web"))

    testImplementation("org.jhades:jhades:1.0.4")
}

tasks {

    shadowJar {
        setProperty("zip64", true)

        exclude(
            listOf(
                "META-INF/*.SF",
                "META-INF/*.DSA",
                "META-INF/*.RSA",

                "scala/*"
            )
        )

        relocate("com.google.common", "repacked.spookystuff.com.google.common")
        relocate("io.netty", "repacked.spookystuff.io.netty")
    }
}
