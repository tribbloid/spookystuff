val vs = versions()

dependencies {

    api(project(":module:core"))
    testFixturesApi(testFixtures(project(":module:core")))
}