val localSettings = file("settings-local.gradle.kts")
if (localSettings.exists()) {
    apply(from = localSettings)
}

val noAssembly: String? by settings
val noBenchmark: String? by settings
// val notebook: String? by settings

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
    ":repack:tika",
    ":repack:selenium",
)

include(":module")

include(":prover-commons")
project(":prover-commons").projectDir = file("prover-commons/module")
include(
    ":prover-commons:infra",
    ":prover-commons:core",
    ":prover-commons:meta2",
    ":prover-commons:spark"
)

include(
    ":module:sanity",
    // uses unstable & experimental scala features, should be modified very slowly & carefully
    ":module:commons",
//    ":module:parsing", // obsolete, moving to inductive graph soon
    ":module:core",

    ":module:linq", // Scala 3 will need a new impl
    ":module:web",
//    ":module:integration",
)

//if (!isEnabled(noAssembly)) {
//    include(
//        ":module:assembly",
//    )
//}
//
//
//if (!isEnabled(noBenchmark)) {
//    include(
//        ":module:benchmark"
//    )
//}

//if (!isEnabled(noUav)) {
//    include(
//        ":module:uav"
//    )
//}
//
