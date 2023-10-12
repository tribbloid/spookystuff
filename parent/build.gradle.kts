
val vs = versions()

plugins {

    scala
    id("io.github.cosmicsilence.scalafix") version "0.1.14"
}

subprojects {

    apply(plugin = "scala")

    configurations.all {

        exclude("net.jpountz.lz4","lz4")
//        exclude("org.codehaus.jettison", "jettison")

        resolutionStrategy {

//            force("org.scala-lang.modules:scala-xml_${vs.scala.binaryV}:1.0.6")
//            force("commons-codec:commons-codec:1.9")
//            force("org.json4s:json4s-jackson_${vs.scala.binaryV}:3.5.5")
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

                val p = this@subprojects

                if (p.hasProperty("notLocal") ) {
                    excludeTags("com.tribbloids.spookystuff.testutils.LocalOnly")
                }
            }

        }
    }

    apply(plugin = "io.github.cosmicsilence.scalafix")
    scalafix {
//        configFile = file("config/myscalafix.conf")
//        includes = ["/com/**/*.scala"]
//        excludes = ["**/generated/**"]
//        ignoreSourceSets = ["scoverage"]

        semanticdb.autoConfigure.set(true)
        semanticdb.version.set("4.8.11")
    }

    dependencies {

        constraints {

            // TODO: some of the following may no longer be necessary
            api("org.apache.httpcomponents:httpclient:4.5.2")

//            api("com.google.guava:guava:16.0.1")
            api("org.apache.commons:commons-compress:1.19")
            api("com.google.protobuf:protobuf-java:2.5.0")

//            api("org.scala-lang.modules:scala-xml_${vs.scala.binaryV}") {
//                version {
//                    strictly ("1.3.0")
//                }
////                because("used by json4s-jackson")
//            }

            api("com.fasterxml.jackson.core:jackson-core:${vs.jacksonV}")
            api("com.fasterxml.jackson.core:jackson-databind:${vs.jacksonV}")
            api("com.fasterxml.jackson.core:jackson-annotations:${vs.jacksonV}")
            api("com.fasterxml.jackson.module:jackson-module-scala_${vs.scala.binaryV}:${vs.jacksonV}")
        }

        // see https://github.com/gradle/gradle/issues/13067
        fun both(notation: Any) {
            implementation(notation)
            testFixturesImplementation(notation)
        }

//        both("${vs.scala.group}:scala-compiler:${vs.scala.v}")
        both("${vs.scala.group}:scala-library:${vs.scala.v}")
        both("${vs.scala.group}:scala-reflect:${vs.scala.v}")

        both("org.apache.spark:spark-sql_${vs.scala.binaryV}:${vs.sparkV}")
        both("org.apache.spark:spark-mllib_${vs.scala.binaryV}:${vs.sparkV}")

        api("org.scala-lang.modules:scala-collection-compat_${vs.scala.binaryV}:2.11.0")

        testRuntimeOnly("org.apache.spark:spark-yarn_${vs.scala.binaryV}:${vs.sparkV}")

        testFixturesApi("org.scalatest:scalatest_${vs.scala.binaryV}:${vs.scalaTestV}")
//        testFixturesApi(project(":repack:scalatest-repack", configuration = "shadow"))
        testFixturesApi("org.junit.jupiter:junit-jupiter:5.10.0")

        // TODO: alpha project, switch to mature solution once https://github.com/scalatest/scalatest/issues/1454 is solved
        testRuntimeOnly("co.helmethair:scalatest-junit-runner:0.2.0")
//        testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.0-SNAPSHOT")
//        testRuntimeOnly("org.junit.platform:junit-platform-engine:1.10.0-SNAPSHOT")

//        testImplementation("org.scalacheck:scalacheck_${vs.scala.binaryV}:1.17.0")

        testImplementation("com.lihaoyi:fastparse_${vs.scala.binaryV}:3.0.2")

        testImplementation("com.vladsch.flexmark:flexmark:0.64.8")
    }
}
