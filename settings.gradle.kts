
val noAssembly: String? by settings
val noBenchmark: String? by settings
val noUav: String? by settings

fun isEnabled(profile: String?): Boolean {
    val result = profile.toBoolean() || profile == ""
    return result
}

//include("graph-commons")
//project(":graph-commons").projectDir = file("graph-commons/core") TODO: enable later

include(
    // should be skipped on CI, contains local experiments only
    ":repack",
    ":repack:selenium-repack",
//    ":repack:json4s-repack",
//    ":repack:scalatest-repack",

    // uses unstable & experimental scala features, should be modified very slowly & carefully
    ":parent",
    ":parent:mldsl",
    ":parent:core",
    ":parent:web",
    ":parent:integration",
    ":spookystuff-showcase"
)

if (!isEnabled(noAssembly)) {
    include(
        ":parent:assembly",
    )
}


if (!isEnabled(noBenchmark)) {
    include(
        ":parent:benchmark"
    )
}

//if (!isEnabled(noUav)) {
//    include(
//        ":parent:uav"
//    )
//}

pluginManagement.repositories {
    gradlePluginPortal()
    mavenCentral()
    // maven("https://dl.bintray.com/kotlin/kotlin-dev")
}
