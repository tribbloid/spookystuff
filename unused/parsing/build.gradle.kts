val vs = versions()

dependencies {

    api(project(":parent:commons"))
    testFixturesApi(testFixtures(project(":parent:commons")))
}
