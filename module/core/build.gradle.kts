val vs = versions()


dependencies {

    api(project(":module:commons"))
    testFixturesApi(testFixtures(project(":module:commons")))

    api(project(":module:linq"))

    testFixturesApi("org.jprocesses:jProcesses:1.6.5")

//    testapi( "com.tribbloids.spookystuff:spookystuff-mldsl:${project.version}"
    api("oauth.signpost:signpost-core:2.1.1")

    // https://mvnrepository.com/artifact/org.typelevel/cats-effect
//    implementation("org.typelevel:cats-effect_${vs.scalaBinaryV}:3.4.2")

    api("com.googlecode.juniversalchardet:juniversalchardet:1.0.3")

    api("org.jsoup:jsoup:1.21.1")
    // TODO: why do I need this? Selenium HtmlUnitDriver should be enough

    api("com.syncthemall:boilerpipe:1.2.2")

    api(project(":repack:tika", configuration = "shadow"))
}
