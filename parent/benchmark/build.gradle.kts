
val vs = versions()

dependencies {

    api(project(":parent:core"))
    testFixturesApi(testFixtures(project(":parent:core")))
}