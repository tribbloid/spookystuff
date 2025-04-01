package com.tribbloids.spookystuff.assembly

import com.tribbloids.spookystuff.commons.classpath.ClasspathResolver
import org.scalatest.funspec.AnyFunSpec

object JarHellDetection {}

class JarHellDetection extends AnyFunSpec {

  // TODO: remove, already have an independent reporting step for every submodule
  ignore("jars conflict") {

    ClasspathResolver.debug { overview =>
      val display = overview.default

      val detected = display.Conflicts.aggregated

      val info = detected
        .map {
          case (k, v) =>
            s"""
                 |${k.mkString("", "\n", "")}:
                 |${v.mkString("\t", "\n\t", "")}
                 |""".stripMargin.trim
        }
        .mkString("\n\n")

      println(info)
    }
  }

//  it("jars conflicts") {
//    val jars = scanner.findOverlappingJars().asScala
//
//    val pairs = jars.map { v =>
//      v.getJar1 -> v.getJar2
//    }
//
//    val conflicting = pairs.filter {
//      case (j1, j2) =>
//        val urls = Seq(j1, j2).map(_.getUrl).distinct
//
//        (urls.size == 2) && //no problem if file path
//        (!urls.exists(url => url.split('/').last == "rt.jar")) && //no problem if conflicting with JVM runtime
//        (j1.getClassLoader == j2.getClassLoader) //no problem if in different class loader
//    }
//
//    val risky = conflicting.flatMap {
//      case (j1, j2) =>
//        val maps = Seq(j1, j2).map { j =>
//          val indexed = j.getResourceVersions.asScala.map { vv =>
//            vv.getResourceName -> vv
//          }
//          val map = indexed.groupBy(_._1).map {
//            case (k, vs) =>
//              val vss = vs.toList
//              assert(vss.size == 1)
//              k -> vss.head._2
//          }
//          map
//        }
//
//        val names = maps.flatMap(_.keys)
//        val classNames = names.filter(_.endsWith(".class"))
//
//        val merged = classNames.map { nn =>
//          nn -> (
//            maps.head.get(nn) -> maps.last.get(nn)
//          )
//        }
//
//        val implTwice = merged.collect {
//          case (nn, (Some(v1), Some(v2))) =>
//            nn -> (v1 -> v2)
//        }
//
//        val implDiff: Seq[(String, (ClasspathResourceVersion, ClasspathResourceVersion))] = implTwice.filter {
//          case (nn, (v1, v2)) =>
//            v1.getFileSize != v2.getFileSize
//        }
//
//        val implDiffSizes: Seq[(String, (Long, Long))] = implDiff.map {
//          case (nn, (v1, v2)) =>
//            nn -> (v1.getFileSize -> v2.getFileSize)
//        }
//
//        val count = implDiff.size
//
//        if (count > 0) Some((j1 -> j2) -> count)
//        else None
////        j1.getResourceVersions.size() != j2.getResourceVersions.size()
//      //TODO: probably not a strong evidence
//
//      //        j1.findManifestClasspathEntries()
//    }
//
//    val msgs = risky.map {
//      case ((j1, j2), count) =>
//        val seq = Seq(j1, j2)
//        val msgs = seq.map { jar =>
//          val parts = Seq(
//            jar.getUrl,
//            s"$count differences"
//          )
//
//          parts.mkString("\t:\t")
//        }
//
//        (msgs ++ Seq("")).mkString("\n")
//    }
//
////    msgs.foreach(println)
//
////    println(
////      s"""
////         |=== SUMMARY ===
////         |discovered:   ${pairs.size}
////         |conflicting:  ${conflicting.size}
////         |risky:        ${risky.size}
////         |""".stripMargin
////    )
//    // TODO: need assertion
//  }

//  it("overlapping jars") {
//
//    _h.overlappingJarsReport()
//  }
//
//  it("multiple versions") {
//    _h.multipleClassVersionsReport(true)
//  }
}
