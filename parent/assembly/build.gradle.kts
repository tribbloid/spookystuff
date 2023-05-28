
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

        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")

        relocate("com.google.common", "repacked.spookystuff.com.google.common")
        relocate("io.netty", "repacked.spookystuff.io.netty")
    }
}
