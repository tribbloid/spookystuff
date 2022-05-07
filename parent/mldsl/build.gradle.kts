
val vs = versions()

dependencies {
    api("org.scalameta:ascii-graphs_${vs.scalaBinaryV}:0.1.2")
    api("io.github.classgraph:classgraph:4.8.149")
//    api(project(":repack:json4s-repack", configuration = "shadow"))
}