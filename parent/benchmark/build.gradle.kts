
val vs = versions()

dependencies {

    api(project(":parent:unused"))
    testFixturesApi(testFixtures(project(":parent:core")))
}