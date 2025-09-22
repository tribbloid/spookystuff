val vs = versions()

dependencies {
    
    api(project(":prover-commons:core"))
    testFixturesApi(testFixtures(project(":prover-commons:core")))
}