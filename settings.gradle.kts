val noAssembly: String? by settings
val noBenchmark: String? by settings
val noUnused: String? by settings
val noUav: String? by settings

pluginManagement.repositories {
    gradlePluginPortal()
    mavenCentral()
    // maven("https://dl.bintray.com/kotlin/kotlin-dev")
}

fun isEnabled(profile: String?): Boolean {
    val result = profile.toBoolean() || profile == ""
    return result
}

include(
    // should be skipped on CI, contains local experiments only
    ":repack",
    ":repack:selenium"
)

include(":parent")

include(":prover-commons")
project(":prover-commons").projectDir = file("prover-commons/module")
include(
    ":prover-commons:core",
    ":prover-commons:meta2",
    ":prover-commons:spark"
)

include(

    // uses unstable & experimental scala features, should be modified very slowly & carefully
    ":parent:commons",
    ":parent:linq",
    ":parent:parsing",
    ":parent:core",
//    ":parent:web",
//    ":parent:integration",
//    ":parent:showcase",
)

//if (!isEnabled(noAssembly)) {
//    include(
//        ":parent:assembly",
//    )
//}
//
//
//if (!isEnabled(noBenchmark)) {
//    include(
//        ":parent:benchmark"
//    )
//}
//
//if (!isEnabled(noUnused)) {
//    include(
//        ":parent:unused"
//    )
//}
//
//if (!isEnabled(noUav)) {
//    include(
//        ":parent:uav"
//    )
//}
//
