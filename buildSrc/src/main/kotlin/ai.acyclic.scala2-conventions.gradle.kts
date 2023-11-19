plugins {

    id("ai.acyclic.scala-mixin")
    id("io.github.cosmicsilence.scalafix")
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

        bothImpl("${vs.scala.group}:scala-library:${vs.scala.v}")
        bothImpl("${vs.scala.group}:scala-reflect:${vs.scala.v}")
//        bothImpl("${vs.scala.group}:scala-compiler:${vs.scala.v}") // enable if low-level mutli-stage programming is required

        testFixturesApi("org.scalatest:scalatest_${vs.scala.binaryV}:${vs.scalaTestV}")

        if (vs.splainV.isNotEmpty()) {
            val splainD = "io.tryp:splain_${vs.scala.v}:${vs.splainV}"
            logger.warn("using ${splainD} ...\t in ${project.displayName} / scalaCompilerPlugins")

            scalaCompilerPlugins(splainD)
        }
    }

    tasks {

        withType<ScalaCompile> {

            scalaCompileOptions.apply {

                additionalParameters.addAll(
                    listOf(
                        "-encoding", "UTF-8",

                        "-g:vars", // demand by json4s

                        "-deprecation",
                        "-unchecked",
                        "-feature",
                        "-language:higherKinds",
                        "-language:existentials",

                        "-Ywarn-value-discard",
                        "-Ywarn-unused:imports",
                        "-Ywarn-unused:implicits",
                        "-Ywarn-unused:params",
                        "-Ywarn-unused:patvars",

                        "-Xlint:poly-implicit-overload",
                        "-Xlint:option-implicit",
//                    ,
//                    "-Xlog-implicits",
//                    "-Xlog-implicit-conversions",
//                    "-Xlint:implicit-not-found",
//                    "-Xlint:implicit-recursion"

                    )
                )

                if (vs.splainV.isNotEmpty()) {
                    additionalParameters.addAll(
                        listOf(
                            "-Vimplicits", "-Vimplicits-verbose-tree", "-Vtype-diffs",
                            "-P:splain:Vimplicits-diverging",
                            "-P:splain:Vtype-detail:4",
                            "-P:splain:Vtype-diffs-detail:3",
//                            "-P:splain:Vdebug"
                        )
                    )
                }
            }

            apply(plugin = "io.github.cosmicsilence.scalafix")
            scalafix {
                semanticdb.autoConfigure.set(true)
                semanticdb.version.set("4.8.11")
            }
        }
    }
}
