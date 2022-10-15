package com.tribbloids.spookystuff.utils.classpath

object DebugClasspath {

  def main(args: Array[String]): Unit = {

    ClasspathResolver.debug { o =>
      println(o.default.completeReport)
    }
  }
}
