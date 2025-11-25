val vs = versions()

dependencies {

    api(project(":module:commons"))
    testFixturesApi(testFixtures(project(":module:commons")))

// https://mvnrepository.com/artifact/org.typelevel/frameless-dataset
    api("org.typelevel:frameless-dataset_${vs.scala.binaryV}:0.16.0")
}
