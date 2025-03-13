
val vs = versions()

dependencies {

    api(project(":parent:core"))
    testFixturesApi(testFixtures(project(":parent:core")))

//    api(project(":repack:selenium", configuration = "shadow"))

    api(project(":repack:selenium"))
}