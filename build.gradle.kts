buildscript {
    repositories {
        // Add here whatever repositories you're already using
        mavenCentral()
    }

    dependencies {
        classpath("ch.epfl.scala:gradle-bloop_2.12:1.6.3") // suffix is always 2.12, weird
    }
}

plugins {
    id("ai.acyclic.scala2-conventions")
    id("ai.acyclic.publish-conventions")

    id("com.gradleup.shadow") version "9.1.0"
}


dependencies {

    runtimeOnly("org.scalameta:scalafmt-interfaces:3.9.9")// only used for prompting upgrade
}


allprojects {
    idea {

        module {
            excludeDirs = excludeDirs + files(
                "temp",

                // apache spark
                "warehouse",
            )
        }
    }
}

idea {

    module {

        excludeDirs = excludeDirs + files(
            ".gradle",

            "prover-commons",
        )
    }
}
