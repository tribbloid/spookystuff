package com.tribbloids.spookystuff.utils

import org.apache.commons.lang.StringUtils

import java.io.InputStream
import java.net.{URL, URLClassLoader}
import scala.language.implicitConversions
import scala.tools.nsc.io.File

trait ClasspathDebugger {

  def entries: Array[String]

  lazy val fileNames: Array[String] = {
    val fileName = entries.flatMap { v =>
      StringUtils.split(v, File.separator).lastOption
    }
    val sorted = fileName.sorted
    sorted
  }

  lazy val formatted: String = fileNames.mkString("\n")

  def main(args: Array[String]): Unit = {

    println(formatted)
  }
}

object ClasspathDebugger {

  case class ForSparkEnv(str: String) extends ClasspathDebugger {

    lazy val entries: Array[String] = {
      val rows = str.split('\n').filter(_.nonEmpty)

      val paths = rows.map(v => v.split('\t').head)

      paths
    }
  }

  case class ForLoader(loader: ClassLoader) extends ClasspathDebugger {

    lazy val entries: Array[String] = {
      loader match {
        case v: URLClassLoader =>
          v.getURLs.map(_.getPath)

        case _ =>
//          loader.asInstanceOf[URLClassLoader].getURLs.map(_.getPath)
          throw new UnsupportedOperationException(s"cannot read from ${loader.getClass}")
      }
    }

    def getResource(str: String): Option[URL] =
      Option(loader.getResource(str.stripSuffix(File.separator)))

    def getResourceAsStream(str: String): Option[InputStream] =
      Option(loader.getResourceAsStream(str.stripSuffix(File.separator)))

    def getResourceDebugInfo(str: String): String = {
      val urlOpt = getResource(str)
      val info = urlOpt match {
        case Some(url) =>
          s"\tresource `$str` refers to ${url.toString}"
        case None =>
          s"\tresource `$str` has no reference"
      }
      this.formatted
      info
    }

    def debugResource(
        fileNames: Seq[String] = List(
          "log4j.properties",
          "rootkey.csv",
          ".rootkey.csv"
        )
    ): Unit = {

      {
        val resolvedInfos = fileNames.map { v =>
          getResourceDebugInfo(v)
        }
        println("resolving files in classpath ...\n" + resolvedInfos.mkString("\n"))
      }
    }
  }

  lazy val system: ForLoader = {

    ForLoader(ClassLoader.getSystemClassLoader)
  }

  implicit def toSystem(v: this.type): ForLoader = v.system
}
