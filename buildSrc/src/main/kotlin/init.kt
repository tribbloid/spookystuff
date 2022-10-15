import org.gradle.api.Project

// TODO: blocked by https://stackoverflow.com/questions/70146737/in-gradle-7-x-is-it-possible-to-invoke-function-defined-in-org-gradle-kotlin-d
//import org.gradle.api.artifacts.dsl.DependencyHandler
//import org.gradle.kotlin.dsl.`implementation`

/**
 * Configures the current project as a Kotlin project by adding the Kotlin `stdlib` as a dependency.
 */
fun Project.versions(): Versions {

    return Versions(this)
}

//fun DependencyHandler.bothImpl(constraintNotation: Any) {
//    implementation(constraintNotation)
//    testFixturesImplementation(constraintNotation)
//}