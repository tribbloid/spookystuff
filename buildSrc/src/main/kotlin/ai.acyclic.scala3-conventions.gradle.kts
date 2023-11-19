plugins {

    id("ai.acyclic.scala-mixin")
}

val vs = versions()

// see https://github.com/gradle/gradle/issues/13067
// TODO: how do I move it into upstream plugins?
fun DependencyHandler.bothImpl(dependencyNotation: Any): Unit {
    implementation(dependencyNotation)
    testFixturesImplementation(dependencyNotation)
}

allprojects {

    dependencies {

        bothImpl("org.scala-lang:scala3-library_3:${vs.scala.v}")
        bothImpl("org.scala-lang:scala3-staging_3:${vs.scala.v}")

        testFixturesApi("org.scalatest:scalatest_3:${vs.scalaTestV}")
    }

    tasks {

        withType<ScalaCompile> {

            scalaCompileOptions.apply {

                additionalParameters.addAll(
                    listOf(

                        "-encoding", "UTF-8",

                        "-verbose", "-explain",

//                        "-language:experimental.dependent"
                    )
                )
            }
        }
    }
}
