
val vs = versions()

plugins {

    scala
    id("io.github.cosmicsilence.scalafix") version "0.1.14"
}

allprojects {

    apply(plugin = "scala")
    apply(plugin = "io.github.cosmicsilence.scalafix")

    configurations.all {

        exclude("net.jpountz.lz4","lz4")
//        exclude("org.codehaus.jettison", "jettison")

        resolutionStrategy {

//            force("org.scala-lang.modules:scala-xml_${vs.scalaBinaryV}:1.0.6")
//            force("commons-codec:commons-codec:1.9")
//            force("org.json4s:json4s-jackson_${vs.scalaBinaryV}:3.5.5")
        }
    }

    sourceSets {
        main {
            scala {
                setSrcDirs(srcDirs + listOf("src/main/java"))
            }
            java {
                setSrcDirs(emptyList<String>())
            }
        }

        testFixtures {
            scala {
                setSrcDirs(srcDirs + listOf("src/testFixtures/java"))
            }
            java {
                setSrcDirs(emptyList<String>())
            }
        }

        test {
            scala {
                setSrcDirs(srcDirs + listOf("src/test/java"))
            }
            java {
                setSrcDirs(emptyList<String>())
            }
        }
    }

    tasks {

        withType<ScalaCompile> {

//            targetCompatibility = jvmTarget

            scalaCompileOptions.apply {

                loggingLevel = "verbose"

                val compilerOptions =

                        mutableListOf(

                                "-encoding", "UTF-8",
                                "-unchecked", "-deprecation", "-feature",

                                // CAUTION: DO NOT DOWNGRADE:
                                // json4s, jackson-scala & paranamer depends on it
                                "-g:vars",

                                "-language:higherKinds",

                                "-Xlint",
                                "-Ywarn-unused",

//                        "-Wunused:imports",

//                        "-Ylog",
//                        "-Ydebug",
//                        "-Vissue", "-Yissue-debug",

//                    "-Xlog-implicits",
//                    "-Xlog-implicit-conversions",
//                    "-Xlint:poly-implicit-overload",
//                    "-Xlint:option-implicit",
//                    "-Xlint:implicit-not-found",
//                    "-Xlint:implicit-recursion"
                        )

//                if (vs.splainV.isNotEmpty()) {
//                    compilerOptions.addAll(
//                        listOf(
//                            "-Vimplicits", "-Vimplicits-verbose-tree", "-Vtype-diffs"
//                        )
//                    )
//                }

                additionalParameters = compilerOptions

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

            testLogging {

                showExceptions = true
                exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL

                showCauses = true
                showStackTraces = true

                // stdout is used for occasional manual verification
                showStandardStreams = true
            }

            useJUnitPlatform {
                includeEngines("scalatest")
                testLogging {
                    events("passed", "skipped", "failed")
                }

                val p = this@allprojects

                if (p.hasProperty("notLocal") ) {
                    excludeTags("com.tribbloids.spookystuff.testutils.LocalOnly")
                }
            }

        }
    }

    dependencies {

        constraints {

            // TODO: some of the following may no longer be necessary
            api("org.apache.httpcomponents:httpclient:4.5.2")

            api("com.google.guava:guava:16.0.1")
            api("org.apache.commons:commons-compress:1.19")
            api("com.google.protobuf:protobuf-java:2.5.0")

//            api("org.scala-lang.modules:scala-xml_${vs.scalaBinaryV}") {
//                version {
//                    strictly ("1.3.0")
//                }
////                because("used by json4s-jackson")
//            }

            api("com.fasterxml.jackson.core:jackson-core:${vs.jacksonV}")
            api("com.fasterxml.jackson.core:jackson-databind:${vs.jacksonV}")
            api("com.fasterxml.jackson.core:jackson-annotations:${vs.jacksonV}")
            api("com.fasterxml.jackson.module:jackson-module-scala_${vs.scalaBinaryV}:${vs.jacksonV}")
        }

        // see https://github.com/gradle/gradle/issues/13067
        fun bothImpl(constraintNotation: Any) {
            implementation(constraintNotation)
            testFixturesApi(constraintNotation)
        }

        fun bothProvided(constraintNotation: Any) {
            compileOnlyApi(constraintNotation)
            testFixturesApi(constraintNotation)
        }

//        api("org.json4s:json4s-jackson_${vs.scalaBinaryV}:3.5.5")

        bothProvided("${vs.scalaGroup}:scala-compiler:${vs.scalaV}")
        bothProvided("${vs.scalaGroup}:scala-library:${vs.scalaV}")
        bothProvided("${vs.scalaGroup}:scala-reflect:${vs.scalaV}")

        bothProvided("org.apache.spark:spark-sql_${vs.scalaBinaryV}:${vs.sparkV}")
        bothProvided("org.apache.spark:spark-mllib_${vs.scalaBinaryV}:${vs.sparkV}")

        testRuntimeOnly("org.apache.spark:spark-yarn_${vs.scalaBinaryV}:${vs.sparkV}")

        testFixturesApi("org.scalatest:scalatest_${vs.scalaBinaryV}:${vs.scalaTestV}")
//        testFixturesApi(project(":repack:scalatest-repack", configuration = "shadow"))
        testFixturesApi("org.junit.jupiter:junit-jupiter:5.9.1")

        // TODO: alpha project, switch to mature solution once https://github.com/scalatest/scalatest/issues/1454 is solved
        testRuntimeOnly("co.helmethair:scalatest-junit-runner:0.2.0")
//        testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.0-SNAPSHOT")
//        testRuntimeOnly("org.junit.platform:junit-platform-engine:1.10.0-SNAPSHOT")

//        testImplementation("org.scalacheck:scalacheck_${vs.scalaBinaryV}:1.17.0")

        testImplementation("com.lihaoyi:fastparse_${vs.scalaBinaryV}:2.3.3")

        testImplementation("com.vladsch.flexmark:flexmark:0.62.2")
    }
}
