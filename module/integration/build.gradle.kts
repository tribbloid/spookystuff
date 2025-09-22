
val vs = versions()

dependencies {

    api(project(":module:web"))
    testFixturesApi(testFixtures(project(":module:core")))
}