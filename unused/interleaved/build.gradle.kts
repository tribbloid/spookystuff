
val vs = versions()

dependencies {

    api(project(":module:core"))
    testFixturesApi(testFixtures(project(":module:core")))

    api("org.mapdb:mapdb:3.1.0") // Don't upgrade! last version compatible with Java 8
}