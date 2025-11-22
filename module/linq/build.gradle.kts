val vs = versions()

dependencies {

    api(project(":module:commons"))
    testFixturesApi(testFixtures(project(":module:commons")))

// https://mvnrepository.com/artifact/com.sparkutils/frameless-dataset_4.0
    api("com.sparkutils:frameless-dataset_${vs.spark.binaryV}_${vs.scala.binaryV}:1.0.0")
}
