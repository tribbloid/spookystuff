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

    id("com.gradleup.shadow") version "8.3.6"
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
