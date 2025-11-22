import org.gradle.api.plugins.ExtraPropertiesExtension
import java.util.Properties
import kotlin.apply

fun loadPropertiesTo(extras: ExtraPropertiesExtension, projectRootDir: File) {
    val localPropertiesFile = File(projectRootDir, "gradle-local.properties")
    if (localPropertiesFile.exists() && localPropertiesFile.isFile && localPropertiesFile.canRead()) {
        localPropertiesFile.inputStream().use { inputStream ->
            Properties().apply { load(inputStream) }
                .forEach { key, value ->
                    if (key != null && value != null) {
                        extras[key.toString()] = value.toString()
                    }
                }
        }
    }
}

// 1. Load into the current context (Settings or Project)
loadPropertiesTo(extra, rootDir)

// 2. Propagate to projects (useful when applied in settings.gradle.kts)
gradle.beforeProject {
    loadPropertiesTo(extra, rootDir)
}

val localSettings = file("settings-local.gradle.kts")
if (localSettings.exists()) {
    apply(from = localSettings)
}

val noAssembly: String? by settings
val noBenchmark: String? by settings

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

if (!isEnabled(noAssembly)) {
    include(
        ":module:assembly",
    )
}


if (!isEnabled(noBenchmark)) {
    include(
        ":module:benchmark"
    )
}
