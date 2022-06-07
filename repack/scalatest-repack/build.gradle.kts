val vs = versions()

dependencies {

    api("org.scalatest:scalatest_${vs.scalaBinaryV}:${vs.scalaTestV}")
}

tasks {
    shadowJar {
        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")

        relocate("scala.xml", "repacked.spookystuff.scala.xml")
//        relocate("com.fasterxml.jackson", "repacked.spookystuff.com.fasterxml.jackson")
    }
}
