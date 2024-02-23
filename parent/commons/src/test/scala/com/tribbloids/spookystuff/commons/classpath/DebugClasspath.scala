package com.tribbloids.spookystuff.commons.classpath

import com.tribbloids.spookystuff.commons.classpath.ClasspathResolver

object DebugClasspath {

  def main(args: Array[String]): Unit = {

    ClasspathResolver.debug { o =>
      println(o.default.completeReport)
    }
  }
}
