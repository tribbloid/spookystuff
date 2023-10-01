import org.gradle.api.Project

class Versions(private val self: Project) {

    // TODO : how to group them?
    val projectGroup = "com.tribbloids.spookstuff"
    val projectRootID = "spookstuff"

    val projectV = "0.10.0-SNAPSHOT"
    val projectVMajor = projectV.removeSuffix("-SNAPSHOT")
//    val projectVComposition = projectV.split('-')

    inner class Scala {
        val group: String = self.properties["scalaGroup"].toString()

        val v: String = self.properties["scalaVersion"].toString()
        protected val vParts: List<String> = v.split('.')

        val binaryV: String = vParts.subList(0, 2).joinToString(".")
        val minorV: String = vParts[2]
    }
    val scala = Scala()

    val shapelessV: String = "2.3.7"

    val scalaTestV: String = "3.2.12"

    val splainV: String = self.properties.get("splainVersion")?.toString() ?: ""

    val sparkV: String = self.properties.get("sparkVersion").toString()

    val tikaV: String = "2.9.0"

    val jacksonV: String = "2.12.3"
}
