val vs = versions()

dependencies {

    api(project(":parent:commons"))
    testFixturesApi(testFixtures(project(":parent:commons")))

//    testFixturesApi(testFixtures(project(":parent:core"))) // only for testing DSL with spookystuff DataView
}
