
val vs = versions()

dependencies {

    api(project(":parent:core"))
    api(project(":parent:parsing"))
    testFixturesApi(testFixtures(project(":parent:core")))
}