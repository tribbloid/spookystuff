package com.tribbloids.spookystuff.utils.classpath

import io.github.classgraph.{ClassGraph, ScanResult}
import org.apache.commons.lang.StringUtils

import java.net.URL
import java.util.regex.Pattern
import scala.collection.mutable
import scala.language.implicitConversions
import scala.tools.nsc.io.File

case class ClasspathDebugger(
    elementsOverride: Option[Seq[String]] = None,
    classLoaderOverride: Option[ClassLoader] = None,
    conflictingResourceFilter: String => Boolean = { v =>
      val exceptions = Set.empty[String]
      v.endsWith(".class") && (!exceptions.contains(v))
    }
) {

  import scala.collection.JavaConverters._

  lazy val graph: ClassGraph = {
    var base = new ClassGraph().enableClassInfo.ignoreClassVisibility

    elementsOverride.foreach { oo =>
      base = base.overrideClasspath(oo)
    }

    classLoaderOverride.foreach { oo =>
      base = base.overrideClassLoaders(oo)
    }

    base
  }

  lazy val dependencyRoots: Seq[String] = Seq(
    ".m2/repository",
    ".gradle/caches",
    "jre/lib",
    "jdk/lib"
  )

  def prunePath(v: String): String = {

    for (root <- dependencyRoots) {

      if (v.contains(root))
        return v.split(Pattern.quote(root)).last
    }

    v
  }

  case class Exe() {

    def scan[T](fn: ScanResult => T): T = {
      val s = graph.scan()
      try {
        fn(s)
      } finally {
        s.close()
      }
    }

    def getResource(path: String): Option[URL] = scan { scanned =>
      scanned
        .getResourcesWithPath(path.stripSuffix(File.separator))
        .asScala
        .map { v =>
          v.getURL
        }
        .headOption
    }

    def getResourceDebugInfo(str: String): String = {
      val urlOpt = getResource(str)
      val info = urlOpt match {
        case Some(url) =>
          s"\tresource `$str` refers to ${url.toString}"
        case None =>
          s"\tresource `$str` has no reference"
      }
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

    object Files {

      lazy val paths: Seq[String] = scan { scanned =>
        elementsOverride.getOrElse {
          scanned.getClasspathURIs.asScala.toList.map(_.toString)
        }
      }

      lazy val prunedPaths: Seq[String] = {

        val pruned = paths.map { v =>
          prunePath(v)
        }
        val sorted = pruned.sorted
        sorted

      }

      lazy val names: Seq[String] = {
        val name = paths.flatMap { v =>
          StringUtils.split(v, File.separator).lastOption
        }
        val sorted = name.sorted
        sorted
      }

      lazy val formatted: String = names.mkString("\n")
    }

    object Conflicts {

      lazy val raw: Map[String, List[String]] = scan { scanned =>
        val seen = mutable.Map.empty[String, mutable.ArrayBuffer[String]]
        try {

          try {
            val resources = scanned.getAllResources.asScala
            for (resource <- resources) {
              val path = resource.getPath
              val classpathElements = seen.getOrElseUpdate(path, mutable.ArrayBuffer.empty)

              classpathElements += resource.getClasspathElementFile.getPath
            }
          } finally {

            if (scanned != null) scanned.close()
          }
        }

        val result = seen.toMap.flatMap {
          case (k, v) =>
            if (!conflictingResourceFilter(k)) None
            else if (v.size == 1) None
            else Some(k -> v.toList.sorted)
        }

        result
      }

      lazy val aggregated: Map[List[String], List[String]] = raw.toList
        .groupBy {
          case (_, vs) =>
            vs
        }
        .map { kvs =>
          kvs._2.map(_._1).sorted -> kvs._1
        }

      lazy val formatted: String = {

        val info = aggregated
          .map {
            case (k, v) =>
              s"""
                 |${k.mkString("", "\n", "")}:
                 |${v.mkString("\t", "\n\t", "")}
                 |""".stripMargin.trim
          }
          .mkString("\n\n")
        info
      }
    }

    lazy val fullReport: String =
      s"""
         | === CONFLICTS ===
         |${Conflicts.formatted}
         | 
         | === FILES ====
         |${Files.formatted}
         |""".stripMargin.trim
  }

  def main(args: Array[String]): Unit = {

    val run = Exe()
    println(run.fullReport)
  }
}

object ClasspathDebugger {

  object default extends ClasspathDebugger()

  object system extends ClasspathDebugger(classLoaderOverride = Some(ClassLoader.getSystemClassLoader))

  class ForSparkEnv(info: String) extends ClasspathDebugger {
    val elements: Seq[String] = {
      val rows = info.split('\n').filter(_.nonEmpty)

      val paths = rows.map(v => v.split('\t').head)
      paths.toSeq
    }
    ClasspathDebugger(
      elementsOverride = Some(elements)
    )
  }

  implicit def toDefault(v: this.type): ClasspathDebugger = v.default
}
