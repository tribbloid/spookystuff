
val vs = versions()

//preBuild.dependsOn(":parent:mldsl")

dependencies {

    api(project(":parent:mldsl"))
    testFixturesApi(testFixtures(project(":parent:mldsl")))

    testFixturesApi("org.jprocesses:jProcesses:1.6.5")

//    testapi( "com.tribbloids.spookystuff:spookystuff-mldsl:${project.version}"
    api("oauth.signpost:signpost-core:2.1.1")
    api("org.apache.commons:commons-csv:1.9.0")
    // https://mvnrepository.com/artifact/org.typelevel/cats-effect
    implementation("org.typelevel:cats-effect_${vs.scalaBinaryV}:3.4.2")

    api("org.apache.tika:tika-core:${vs.tikaV}")
    testImplementation( "org.apache.tika:tika-parsers-standard-package:${vs.tikaV}")
    api("com.googlecode.juniversalchardet:juniversalchardet:1.0.3")
    api("org.jsoup:jsoup:1.15.3")
    api("com.syncthemall:boilerpipe:1.2.2")
}
