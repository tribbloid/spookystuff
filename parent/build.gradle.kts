
val vs = versions()

val sparkV = "3.5.0"
val jacksonV = "2.12.3"

plugins {
//    id("ai.acyclic.scala2-conventions")
}

subprojects {


    tasks {

        withType<ScalaCompile> {

            scalaCompileOptions.additionalParameters.addAll(
                listOf(

                    "-Wconf:cat=deprecation:ws"
                )
            )
        }
    }

    // these are either useless or should be merged into dependencies/constraints
    configurations.all {
//        exclude("net.jpountz.lz4","lz4")
        resolutionStrategy {
//            force("commons-codec:commons-codec:1.9")
        }
    }

    dependencies {

        bothImpl("org.apache.spark:spark-sql_${vs.scala.binaryV}:${sparkV}")
        bothImpl("org.apache.spark:spark-mllib_${vs.scala.binaryV}:${sparkV}")

        testRuntimeOnly("org.apache.spark:spark-yarn_${vs.scala.binaryV}:${sparkV}")

        testImplementation("com.lihaoyi:fastparse_${vs.scala.binaryV}:3.0.2")

        testImplementation("com.vladsch.flexmark:flexmark:0.64.8")


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

            api("com.fasterxml.jackson.core:jackson-core:${jacksonV}")
            api("com.fasterxml.jackson.core:jackson-databind:${jacksonV}")
            api("com.fasterxml.jackson.core:jackson-annotations:${jacksonV}")
            api("com.fasterxml.jackson.module:jackson-module-scala_${vs.scala.binaryV}:${jacksonV}")
        }
    }


    tasks {

        test {

            useJUnitPlatform {

                val p = this@subprojects

                if (p.hasProperty("notLocal") ) {
                    excludeTags("com.tribbloids.spookystuff.testutils.LocalOnly")
                }
            }
        }
    }

}