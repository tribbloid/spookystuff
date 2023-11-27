buildscript {
    repositories {
        // Add here whatever repositories you're already using
        mavenCentral()
    }
}

plugins {
    id("ai.acyclic.scala2-conventions")
    id("ai.acyclic.publish-conventions")

    id("com.github.johnrengelman.shadow") version "8.1.1"
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
