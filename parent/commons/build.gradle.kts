val vs = versions()

dependencies {

    api(project(":prover-commons:spark"))
    testFixturesApi(testFixtures(project(":prover-commons:spark")))

    api("io.github.classgraph:classgraph:4.8.179")

    api("com.lihaoyi:pprint_${vs.scala.binaryV}:0.9.0")

    api("org.scala-lang.modules:scala-collection-compat_${vs.scala.binaryV}:2.12.0")

// https://mvnrepository.com/artifact/org.typelevel/frameless-dataset
    api("org.typelevel:frameless-dataset_${vs.scala.binaryV}:0.16.0")

    testImplementation(project(":prover-commons:meta2"))
}
