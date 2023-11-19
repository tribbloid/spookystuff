buildscript {
    repositories {
        // Add here whatever repositories you're already using
        mavenCentral()
    }
}

plugins {
    id("ai.acyclic.java-conventions")
    id("ai.acyclic.publish-conventions")

    id("com.github.johnrengelman.shadow") version "8.1.1"
}

idea {

    module {

        excludeDirs = excludeDirs + files(
            ".gradle",

            // apache spark
            "warehouse",

            "parent/prover-commons",
        )
    }
}
