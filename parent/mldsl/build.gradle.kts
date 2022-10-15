
val vs = versions()

dependencies {
    api("com.github.mutcianm:ascii-graphs_${vs.scalaBinaryV}:0.0.6")
    api("io.github.classgraph:classgraph:4.8.149")
//    api(project(":repack:json4s-repack", configuration = "shadow"))
}