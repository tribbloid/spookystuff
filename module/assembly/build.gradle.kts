
val vs = versions()

//plugins {
//    id("com.gradleup.shadow")
//}

dependencies {

    api(project(":parent:web"))

    testImplementation("org.jhades:jhades:1.0.4")
}

tasks {

    // TODO: move to BuildSrc as a convention
    assemble {
        dependsOn("copyJars")
    }

    register<Copy>("copyJars") {

        val libsDir = layout.buildDirectory.dir("assembly")

        val paths = configurations.runtimeClasspath.get().filter {
            it.exists()
        }

//        paths.forEach { println(it) }

        from(
            paths
//            paths.map {
//                if (it.isDirectory) it else zipTree(it)
//            }
        )

        into(libsDir)

//        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }


//    shadowJar {
//        setProperty("zip64", true)
//
////        exclude(
////            listOf(
////                "META-INF/*.SF",
////                "META-INF/*.DSA",
////                "META-INF/*.RSA",
////            )
////        )
//
////        relocate("com.google.common", "repacked.spookystuff.com.google.common")
////        relocate("io.netty", "repacked.spookystuff.io.netty")
//    }
}
