val vs = versions()

buildscript {
    repositories {
        // Add here whatever repositories you're already using
        mavenCentral()
    }

//    val vs = versions()

    dependencies {
        classpath("ch.epfl.scala:gradle-bloop_2.12:1.5.3") // suffix is always 2.12, weird
    }
}

plugins {
//    base
    `java-library`
    `java-test-fixtures`

    scala
//    kotlin("jvm") version "1.6.10" // TODO: remove?

    idea

    signing
    `maven-publish`
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"

    id("com.github.ben-manes.versions") version "0.42.0"
    id("project-report")

    id("com.github.johnrengelman.shadow") version "7.1.2"
}

val sonatypeApiUser = providers.gradleProperty("sonatypeApiUser")
val sonatypeApiKey = providers.gradleProperty("sonatypeApiKey")
if (sonatypeApiUser.isPresent && sonatypeApiKey.isPresent) {
    nexusPublishing {
        repositories {
            sonatype {

                nexusUrl.set(uri("https://www.oss.sonatype.org/service/local/"))
                snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))

                username.set(sonatypeApiUser)
                password.set(sonatypeApiKey)
                useStaging.set(true)
            }
        }
    }
} else {
    logger.warn("Sonatype API credential not defined, skipping ...")
}

allprojects {

    apply(plugin = "java-library")

    // apply(plugin = "bloop")
    // DO NOT enable! In VSCode it will cause the conflict:
    // Cannot add extension with name 'bloop', as there is an extension already registered with that name

    apply(plugin = "idea")

    group = vs.projectGroup
    version = vs.projectV

    repositories {
//        mavenLocal()
        mavenCentral()
//        jcenter()
//        maven("https://dl.bintray.com/kotlin/kotlin-dev")
//        maven("https://scala-ci.typesafe.com/artifactory/scala-integration/") // scala SNAPSHOT
    }

    idea {

        module {

            excludeDirs = excludeDirs + files(

                "target",
                "out",

                ".idea",
                ".vscode",
                ".bloop",
                ".bsp",
                ".metals",
                "bin",

                ".ammonite",

                "logs"
            )

            isDownloadJavadoc = true
            isDownloadSources = true
        }
    }
}

subprojects {

    apply(plugin = "java-test-fixtures")
    apply(plugin = "scala")

    apply(plugin = "signing")
    apply(plugin = "maven-publish")
    apply(plugin = "project-report")

    apply(plugin = "com.github.johnrengelman.shadow")
//    apply(plugin = "ru.tinkoff.gradle.jarjar")

    // resolving version conflicts
    // TODO: remove, already defined in `constraints` as below
//    configurations.all {
//        resolutionStrategy.dependencySubstitution {
//            substitute(
//                module("com.chuusai:shapeless_${vs.scalaBinaryV}")
//            ).apply {
//                using(module("com.chuusai:shapeless_${vs.scalaBinaryV}:${vs.shapelessV}"))
//            }
//        }
//    }

//    https://stackoverflow.com/questions/23261075/compiling-scala-before-alongside-java-with-gradle
    sourceSets {
        main {
            scala {
                setSrcDirs(srcDirs + listOf("src/main/java"))
            }
            java {
                setSrcDirs(emptyList<String>())
            }
        }

        testFixtures {
            scala {
                setSrcDirs(srcDirs + listOf("src/testFixtures/java"))
            }
            java {
                setSrcDirs(emptyList<String>())
            }
        }

        test {
            scala {
                setSrcDirs(srcDirs + listOf("src/test/java"))
            }
            java {
                setSrcDirs(emptyList<String>())
            }
        }
    }


    task("dependencyTree") {

        dependsOn("dependencies", "htmlDependencyReport")
    }

    tasks {
//        val jvmTarget = JavaVersion.VERSION_1_8.toString()

        htmlDependencyReport {

            reports.html.outputLocation.set(File("build/reports/dependencyTree/htmlReport"))
        }

        withType<ScalaCompile> {

//            targetCompatibility = jvmTarget

            scalaCompileOptions.apply {

                loggingLevel = "verbose"

                val compilerOptions =

                    mutableListOf(

                        "-encoding", "UTF-8",
                        "-unchecked", "-deprecation", "-feature",

                        // CAUTION: DO NOT DOWNGRADE:
                        // json4s, jackson-scala & paranamer depends on it
                        "-g:vars",

                        "-language:higherKinds",

                        "-Xlint",
                        "-Ywarn-unused",

//                        "-Wunused:imports",

//                        "-Ylog",
//                        "-Ydebug",
//                        "-Vissue", "-Yissue-debug",

//                    "-Xlog-implicits",
//                    "-Xlog-implicit-conversions",
//                    "-Xlint:poly-implicit-overload",
//                    "-Xlint:option-implicit",
//                    "-Xlint:implicit-not-found",
//                    "-Xlint:implicit-recursion"
                    )

//                if (vs.splainV.isNotEmpty()) {
//                    compilerOptions.addAll(
//                        listOf(
//                            "-Vimplicits", "-Vimplicits-verbose-tree", "-Vtype-diffs"
//                        )
//                    )
//                }

                additionalParameters = compilerOptions

                forkOptions.apply {

                    memoryInitialSize = "1g"
                    memoryMaximumSize = "4g"

                    // this may be over the top but the test code in macro & core frequently run implicit search on church encoded Nat type
                    jvmArgs = listOf(
                        "-Xss256m"
                    )
                }
            }
        }

        withType<AbstractArchiveTask> {

            isPreserveFileTimestamps = false
            isReproducibleFileOrder = true
        }
    }

    java {
        withSourcesJar()
        withJavadocJar()
    }

    // https://stackoverflow.com/a/66352905/1772342
    val signingSecretKey = providers.gradleProperty("signing.gnupg.secretKey")
    val signingKeyPassphrase = providers.gradleProperty("signing.gnupg.passphrase")
    signing {
        useGpgCmd()
        if (signingSecretKey.isPresent) {
            useInMemoryPgpKeys(signingSecretKey.get(), signingKeyPassphrase.get())
//            useInMemoryPgpKeys(signingKeyID.get(), signingSecretKey.get(), signingKeyPassphrase.get())
            sign(extensions.getByType<PublishingExtension>().publications)
        } else {
            logger.warn("PGP signing key not defined, skipping ...")
        }
    }

    publishing {
        val suffix = "_" + vs.scalaBinaryV

        val rootID = vs.projectRootID

        val moduleID =
            if (project.name.equals(rootID)) throw UnsupportedOperationException("root project should not be published")
            else rootID + "-" + project.name + suffix

        val whitelist = setOf("graph-commons", "macro", "core")

        if (whitelist.contains(project.name)) {

            publications {
                create<MavenPublication>("maven") {
                    artifactId = moduleID

                    val javaComponent = components["java"] as AdhocComponentWithVariants
                    from(javaComponent)

                    javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
                    javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }

                    pom {
                        licenses {
                            license {
                                name.set("Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0")
                            }
                        }

                        name.set("spookystuff")
                        description.set(
                            "parallel graph data scraper"
                        )

                        val github = "https://github.com/tribbloid"
                        val repo = github + "/spookystuff"

                        url.set(repo)

                        developers {
                            developer {
                                id.set("tribbloid")
                                name.set("Peng Cheng")
                                url.set(github)
                            }
                        }
                        scm {
                            connection.set("scm:git@github.com:tribbloid/spookystuff")
                            url.set(repo)
                        }
                    }
                }
            }
        }
    }
}

idea {

    module {

        excludeDirs = excludeDirs + files(
            ".gradle",

            // apache spark
            "warehouse"
        )
    }
}
