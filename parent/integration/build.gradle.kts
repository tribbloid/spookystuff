
val vs = versions()

dependencies {

    api(project(":parent:web"))
    testFixturesApi(testFixtures(project(":parent:core")))
}