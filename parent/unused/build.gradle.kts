
val vs = versions()

dependencies {

    api(project(":parent:core"))
    testFixturesApi(testFixtures(project(":parent:core")))

    api("org.mapdb:mapdb:3.1.0") // Don't upgrade! last version compatible with Java 8
}