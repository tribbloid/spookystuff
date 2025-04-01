
val vs = versions()

dependencies {

    api(project(":module:core"))
    testFixturesApi(testFixtures(project(":module:core")))

//    api(project(":repack:selenium", configuration = "shadow"))

    api(project(":repack:selenium"))
}