val vs = versions()

dependencies {

    api(project(":repack:tika", configuration = "shadow"))
    api(project(":repack:selenium", configuration = "shadow"))
}