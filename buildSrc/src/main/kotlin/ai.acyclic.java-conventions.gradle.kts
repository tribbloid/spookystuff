import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import org.gradle.kotlin.dsl.*

plugins {
//    base
    java
    `java-test-fixtures`

    `project-report`
    idea

    id("com.github.ben-manes.versions" )
}

val vs = versions()

// TODO: remove after https://github.com/ben-manes/gradle-versions-plugin/issues/816 resolved
tasks.named<DependencyUpdatesTask>("dependencyUpdates").configure {
    filterConfigurations = Spec<Configuration> {
        !it.name.startsWith("incrementalScalaAnalysis")
    }
}

idea {

    targetVersion = "2023"

    module {

        excludeDirs = excludeDirs + files(
            "gradle",
        )
    }
}

allprojects {

    apply(plugin = "java")
    apply(plugin = "java-library")
    apply(plugin = "java-test-fixtures")

    apply(plugin = "idea")

    group = vs.rootGroup
    version = vs.rootV

    repositories {
        mavenLocal()
        mavenCentral()
//        jcenter()
        maven("https://dl.bintray.com/kotlin/kotlin-dev")
    }

    java {

        val jvmTarget = vs.jvmTarget

        withSourcesJar()
        withJavadocJar()

        sourceCompatibility = jvmTarget
        targetCompatibility = jvmTarget
    }

    idea {

        targetVersion = "2023"

        module {

            excludeDirs = excludeDirs + files(
                "target",
                "out",
                "bin",


                ".gradle",
                ".idea",
                ".vscode",
                ".cache",
                ".history",
                ".lib",

                "logs"
            )

            isDownloadJavadoc = true
            isDownloadSources = true
        }
    }
}

//subprojects {
//
//    apply(plugin = "project-report")
//
//}


task("dependencyTree") {

    dependsOn("dependencies", "htmlDependencyReport")
}

tasks {

    htmlDependencyReport {

        reports.html.outputLocation.set(File("build/reports/dependencyTree/htmlReport"))
    }
}