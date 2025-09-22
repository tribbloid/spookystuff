

plugins {
    id("com.gradleup.shadow")
}

allprojects {

    apply { plugin("com.gradleup.shadow") }


    tasks {
        shadowJar {

            mergeServiceFiles()

            setExcludes(

                listOf(
                    "META-INF/*.SF",
                    "META-INF/*.DSA",
                    "META-INF/*.RSA",

                    "**/scala/**"
                )
            )
        }
    }

}