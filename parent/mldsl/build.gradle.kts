
val vs = versions()

dependencies {

    api("org.scalameta:ascii-graphs_${vs.scala.binaryV}:0.1.2")
    api("io.github.classgraph:classgraph:4.8.163")

    api("com.lihaoyi:pprint_${vs.scala.binaryV}:0.8.1")

    api("org.scala-lang.modules:scala-collection-compat_${vs.scala.binaryV}:2.11.0")
}
