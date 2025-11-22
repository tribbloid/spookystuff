val vs = versions()

dependencies {

    api(project(":prover-commons:spark"))
    testFixturesApi(testFixtures(project(":prover-commons:spark")))

    api("io.github.classgraph:classgraph:4.8.184")

    api("org.scala-lang.modules:scala-collection-compat_${vs.scala.artifactSuffix}:2.14.0")

    testImplementation(project(":prover-commons:meta2"))
}
