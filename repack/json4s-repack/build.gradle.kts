val vs = versions()

dependencies {

    api("org.json4s:json4s-jackson_${vs.scalaBinaryV}:4.0.4")
}
// Kotlin DSL
val jarJar = task<JavaExec>("jarjar") {
    val finalJarFile = "$buildDir/libs/${project.name}-${rootProject.version}.jar"
    main = "-jar"
    args = listOf("path/to/jarjar.jar", "process", "path/to/relocating_rules.txt", finalJarFile, finalJarFile)
}

tasks {
    shadowJar {
        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")

        relocate("org.json4s", "repacked.spookystuff.org.json4s")
        relocate("com.fasterxml.jackson", "repacked.spookystuff.com.fasterxml.jackson")
    }


    withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
        //...//
        finalizedBy(jarJar)
    }
}
