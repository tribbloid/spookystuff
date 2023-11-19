
val vs = versions()

val sparkV = "3.5.0"

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
}

subprojects {

    dependencies {

        bothImpl("org.apache.spark:spark-sql_${vs.scala.binaryV}:${sparkV}")
        bothImpl("org.apache.spark:spark-mllib_${vs.scala.binaryV}:${sparkV}")

        testRuntimeOnly("org.apache.spark:spark-yarn_${vs.scala.binaryV}:${sparkV}")

        testImplementation("com.lihaoyi:fastparse_${vs.scala.binaryV}:3.0.2")

        testImplementation("com.vladsch.flexmark:flexmark:0.64.8")

    }
}