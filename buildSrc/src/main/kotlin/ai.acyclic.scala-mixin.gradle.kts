import org.gradle.api.tasks.scala.ScalaCompile
import org.gradle.kotlin.dsl.*


plugins {

    scala
    id("ai.acyclic.java-conventions")
}

val vs = versions()

allprojects {

    // apply(plugin = "bloop")
    // DO NOT enable! In VSCode it will cause the conflict:
    // Cannot add extension with name 'bloop', as there is an extension already registered with that name

    apply(plugin = "scala")

    dependencies {

        testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
        testRuntimeOnly("co.helmethair:scalatest-junit-runner:0.2.0")
    }

    tasks {

        withType<ScalaCompile> {

            val jvmTarget = vs.jvmTarget.toString()

            sourceCompatibility = jvmTarget
            targetCompatibility = jvmTarget

            scalaCompileOptions.apply {

                loggingLevel = "verbose"

                forkOptions.apply {

                    memoryInitialSize = "1g"
                    memoryMaximumSize = "4g"

                    // this may be over the top but the test code in macro & core frequently run implicit search on church encoded Nat type
                    jvmArgs = listOf(
                        "-Xss256m"
                    )
                }
            }
        }

        test {

            minHeapSize = "1024m"
            maxHeapSize = "4096m"

            useJUnitPlatform {
                includeEngines("scalatest")
                testLogging {
                    events("passed", "skipped", "failed")
                }
            }

            testLogging {
//                events = setOf(org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED, org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED, org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED, org.gradle.api.tasks.testing.logging.TestLogEvent.STANDARD_OUT)
//                exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
                showExceptions = true
                showCauses = true
                showStackTraces = true

                // stdout is used for occasional manual verification
                showStandardStreams = true
            }
        }

    }

    idea {

        module {

            excludeDirs = excludeDirs + listOf(
                file(".bloop"),
                file(".bsp"),
                file(".metals"),
                file(".ammonite"),
            )
        }
    }
}
