import org.gradle.api.publish.maven.MavenPublication
import org.gradle.kotlin.dsl.*

plugins {

    signing
    `maven-publish`

    id("io.github.gradle-nexus.publish-plugin")// version "1.3.0"
}

val vs = versions()

val sonatypeApiUser = providers.gradleProperty("sonatypeApiUser")
val sonatypeApiKey = providers.gradleProperty("sonatypeApiKey")
if (sonatypeApiUser.isPresent && sonatypeApiKey.isPresent) {
    nexusPublishing {
        repositories {
            sonatype {

//                nexusUrl.set(uri("https://oss.sonatype.org/service/local/"))
//                snapshotRepositoryUrl.set(uri("https://oss.sonatype.org/content/repositories/snapshots/"))

                username.set(sonatypeApiUser)
                password.set(sonatypeApiKey)
                useStaging.set(true)
            }
        }
    }
} else {
    logger.warn("Sonatype API key not defined, skipping configuration of Maven Central publishing repository")
}

subprojects {

    apply(plugin = "signing")

    // https://stackoverflow.com/a/66352905/1772342

//    val signingKeyID = providers.gradleProperty("signing.gnupg.keyID")
    val signingSecretKey = providers.gradleProperty("signing.gnupg.secretKey")
    val signingKeyPassphrase = providers.gradleProperty("signing.gnupg.passphrase")

    signing {
        useGpgCmd()
        if (signingSecretKey.isPresent) {
            useInMemoryPgpKeys(signingSecretKey.get(), signingKeyPassphrase.get())
//            useInMemoryPgpKeys(signingKeyID.get(), signingSecretKey.get(), signingKeyPassphrase.get())
            sign(extensions.getByType<PublishingExtension>().publications)
        } else {
            logger.warn("PGP signing key not defined, skipping signing configuration")
        }
    }

    apply(plugin = "maven-publish")
    publishing {
        val rootID = vs.rootID

        val moduleID = if (project.name.startsWith(rootID)) project.name
        else rootID + "-" + project.name

        publications {
            create<MavenPublication>("maven") {
                artifactId = moduleID
                groupId = groupId // TODO: redundant
                version = version

                from(components["java"])

                suppressPomMetadataWarningsFor("testFixturesApiElements")
                suppressPomMetadataWarningsFor("testFixturesRuntimeElements")
            }
        }
    }
}