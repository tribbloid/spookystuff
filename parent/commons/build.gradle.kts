val vs = versions()

dependencies {

    api(project(":prover-commons:spark")) // TODO: incldue meta?
    testFixturesApi(testFixtures(project(":prover-commons:spark")))

    api("io.github.classgraph:classgraph:4.8.170")

    api("com.lihaoyi:pprint_${vs.scala.binaryV}:0.8.1")

    api("org.scala-lang.modules:scala-collection-compat_${vs.scala.binaryV}:2.11.0")

// https://mvnrepository.com/artifact/org.typelevel/frameless-dataset
    api("org.typelevel:frameless-dataset_${vs.scala.binaryV}:0.16.0")

    testImplementation(project(":prover-commons:meta2"))
}
