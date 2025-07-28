val vs = versions()

dependencies {

    api(project(":prover-commons:spark"))
    testFixturesApi(testFixtures(project(":prover-commons:spark")))

    api("io.github.classgraph:classgraph:4.8.181")

    api("org.scala-lang.modules:scala-collection-compat_${vs.scala.artifactSuffix}:2.13.0")

// https://mvnrepository.com/artifact/org.typelevel/frameless-dataset
    api("org.typelevel:frameless-dataset_${vs.scala.binaryV}:0.16.0")

    testImplementation(project(":prover-commons:meta2"))
}
