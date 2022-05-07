import org.gradle.api.Project

class Versions(self: Project) {

    // TODO : how to group them?
    val projectGroup = "com.tribbloids.spookstuff"
    val projectRootID = "spookstuff"

    val projectV = "0.9.0-SNAPSHOT"
    val projectVMajor = projectV.removeSuffix("-SNAPSHOT")
//    val projectVComposition = projectV.split('-')

    val scalaGroup: String = self.properties.get("scalaGroup").toString()

    val scalaV: String = self.properties.get("scalaVersion").toString()

    protected val scalaVParts = scalaV.split('.')

    val scalaBinaryV: String = scalaVParts.subList(0, 2).joinToString(".")
    val scalaMinorV: String = scalaVParts[2]

    val sparkV: String = self.properties.get("sparkVersion").toString()

    val scalaTestV: String = "3.2.10"

    val tikaV: String = "2.4.1"

    val jacksonV: String = "2.12.3"
}
