val vs = versions()

dependencies {

    api(project(":module:commons"))
    testFixturesApi(testFixtures(project(":module:commons")))

}
