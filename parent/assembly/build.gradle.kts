
val vs = versions()

dependencies {

    api(project(":parent:web"))

    testImplementation("org.jhades:jhades:1.0.4")
}

tasks {
    shadowJar {
        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")

        relocate("com.google.common", "repacked.spookystuff.com.google.common")
        relocate("io.netty", "repacked.spookystuff.io.netty")
    }
}
