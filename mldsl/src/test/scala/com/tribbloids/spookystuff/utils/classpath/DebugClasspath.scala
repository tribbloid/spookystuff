package com.tribbloids.spookystuff.utils.classpath

object DebugClasspath {

  def main(args: Array[String]): Unit = {

    ClasspathResolver.default.withOverview { o =>
      println(o.completeReport)
    }
  }
}
