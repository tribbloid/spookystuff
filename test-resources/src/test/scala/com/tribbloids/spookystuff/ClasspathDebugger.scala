package com.tribbloids.spookystuff

import org.apache.commons.lang.StringUtils

import scala.tools.nsc.io.File

case class ClasspathDebugger(classpathStr: String) {

  def main(args: Array[String]): Unit = {

    val rows = classpathStr.split('\n').filter(_.nonEmpty)
    val paths = rows.map(v => v.split('\t').head)
    val fileName = paths.flatMap { v =>
      StringUtils.split(v, File.separator).lastOption
    }
    val sorted = fileName.sorted

    println(sorted.mkString("\n"))
  }
}
