
plugins {
    id("ai.acyclic.scala2-conventions")
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
