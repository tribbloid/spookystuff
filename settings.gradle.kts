
val noAssembly: String? by settings
val noBenchmark: String? by settings
val noUnused: String? by settings
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
    ":repack:selenium-repack"
)

include(":parent")

//include(":parent:prover-commons")
//project(":parent:prover-commons").projectDir = file("parent/prover-commons/module")
//include(
//    ":parent:prover-commons:core",
//    ":parent:prover-commons:meta2"
//)

include(

    // uses unstable & experimental scala features, should be modified very slowly & carefully
    ":parent:mldsl",
    ":parent:core",
    ":parent:web",
    ":parent:integration",
//    ":spookystuff-showcase",
//    ":spookystuff-showcase:showcase",
//    ":spookystuff-showcase:notebook", TODO: enable later
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

if (!isEnabled(noUnused)) {
    include(
        ":parent:unused"
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
