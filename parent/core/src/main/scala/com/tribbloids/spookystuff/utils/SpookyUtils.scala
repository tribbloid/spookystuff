package com.tribbloids.spookystuff.utils

import java.io.File
import java.net._
import java.nio.file.{Files, _}
import com.tribbloids.spookystuff.utils.io.LocalResolver
import org.apache.commons.io.IOUtils
import org.apache.spark.ml.dsl.UnsafeUtils
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

object SpookyUtils {

  import SpookyViews._

  //  def retryWithDeadline[T](n: Int, t: Duration)(fn: => T): T = retry(n){withDeadline(t){fn}}

  def canonizeFileName(
      name: String,
      noDash: Boolean = false
  ): String = {

    var result = name.replaceAll("[ ]", "_").replaceAll("""[^0-9a-zA-Z!_.*'()-]+""", "_")

    if (result.length > 255) result = result.substring(0, 255)
    if (noDash) result = result.replaceAll("-", "_") // phantomJS log will behave erratically if "-" is present

    result
  }

  def canonizeUrn(name: String): String = {

    var result = name.replaceAll("[ ]", "_").replaceAll("""[^0-9a-zA-Z!_.*'()-]+""", "/")

    result = result
      .split("/")
      .map { part =>
        {
          if (part.length > 255) part.substring(0, 255)
          else part
        }
      }
      .mkString("/")

    result
  }

  // Spark SQL does not currently support column names with dots (see SPARK-2775),
  // so we'll need to post-process the inferred schema to convert dots into underscores:
  def canonizeColumnName(name: String): String = {
    name.replaceAllLiterally(".", "_")
  }

  def typedOrNone[B: ClassTag](v: Any): Option[B] = {
    val array =
      try {
        Array[B](v.asInstanceOf[B])
      } catch {
        case _: Exception =>
          Array[B]()
      }
    array.headOption
  }

  // TODO: move to class & try @Specialized?
  def asArray[T <: Any: ClassTag](obj: Any): Array[T] = {

    val canon: Array[_] = obj match {
      case v: IterableOnce[Any] => v.toArray
      case v: Array[_]          => v
      case _                    => Array[Any](obj)
    }

    canon.collect {
      case v: T => v
    }
  }

  def asOption[T <: Any: ClassTag](obj: Any): Option[T] = asIterable[T](obj).headOption

  def asIterable[T <: Any: ClassTag](obj: Any): Iterable[T] = {

    val canon: Iterable[Any] = obj match {
      case v: IterableOnce[Any] => v.iterator.to(Iterable)
      case v: Array[_]          => v.iterator.to(Iterable)
      case _ =>
        Iterable[Any](obj)
    }

    canon.collect {
      case v: T => v
    }
  }

  // TODO: need test, or its already superceded by try catch?
  def javaUnbox(boxed: Any): Any = {
    boxed match {
      case n: java.lang.Byte =>
        n.byteValue()
      case n: java.lang.Short =>
        n.shortValue()
      case n: Character =>
        n.charValue()
      case n: Integer =>
        n.intValue()
      case n: java.lang.Long =>
        n.longValue()
      case n: java.lang.Float =>
        n.floatValue()
      case n: java.lang.Double =>
        n.doubleValue()
      case n: java.lang.Boolean =>
        n.booleanValue()
      case o @ _ =>
        o
    }
  }

  def addCPResource(urlStr: String): Unit = {

    val url = scala.reflect.io.File(urlStr.stripSuffix("/")).toAbsolute.toURL

    assert(url.toString.startsWith("file"))

    UnsafeUtils.invoke(
      classOf[URLClassLoader],
      ClassLoader.getSystemClassLoader,
      "addURL",
      classOf[URL] -> url
    )

    assert(ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader].getURLs.contains(url))
  }

  def resilientCopy(src: Path, dst: Path, options: Array[CopyOption]): Unit = {
    CommonUtils.retry(5, 1000) {

      val pathsStr = src + " => " + dst

      if (Files.isDirectory(src)) {
        try {
          Files.copy(src, dst, options: _*)
          // TODO: how to flush dst?
        } catch {
          case _: DirectoryNotEmptyException =>
        }

        val dstFile = new File(dst.toString)
        assert(dstFile.isDirectory)

        LoggerFactory.getLogger(this.getClass).debug(pathsStr + " no need to copy directory")
      } else {
        Files.copy(
          src,
          dst,
          options: _*
        ) // this will either 1. copy file if src is a file. 2. create empty dir if src is a dir.
        // TODO: how to flush dst?

        // assert(Files.exists(dst))
        // NIO copy should use non-NIO for validation to eliminate stream caching
        val dstContent = LocalResolver.input(dst.toString) { in =>
          IOUtils.toByteArray(in.stream)
        }
        //      assert(srcContent.length == dstContent.length, pathsStr + " copy failed")
        LoggerFactory.getLogger(this.getClass).debug(pathsStr + s" ${dstContent.length} byte(s) copied")
      }
    }
  }

  def treeCopy(srcPath: Path, dstPath: Path): Any = {

    Files.walkFileTree(
      srcPath,
      java.util.EnumSet.of(FileVisitOption.FOLLOW_LINKS),
      Integer.MAX_VALUE,
      new CopyDirectoryFileVisitor(srcPath, dstPath)
    )
  }
  def ifFileNotExist[T](dst: String)(f: => T): Option[T] = this.synchronized {
    val dstFile = new File(dst)
    if (!dstFile.exists()) {
      Some(f)
    } else {
      None
    }
  }

  // TODO: this is not tested on workers
//  def extractResource(resource: URL, dst: String): Unit = {
//
//    resource.getProtocol match {
//      case "jar" =>
//        val fullPath = resource.toString
//        val split = fullPath.split('!')
//        assert(split.length == 2)
//        val jarPath = split.head
//        val innerPathStr = split.last
//
//        val fs = FileSystems.newFileSystem(new URI(jarPath), new java.util.HashMap[String, String]())
//        try {
//          val srcPath = fs.getPath(innerPathStr)
//
//          treeCopy(srcPath, new File(dst).toPath)
//        } finally {
//          fs.close()
//        }
//      case _ =>
//        val src = new File(resource.toURI)
//        treeCopy(src.toPath, new File(dst).toPath)
//    }
//  }

  def longHash(string: String): Long = {
    var h: Long = 1125899906842597L // prime
    val len: Int = string.length
    var i: Int = 0
    while (i < len) {
      {
        h = 31 * h + string.charAt(i)
      }
      {
        i += 1; i - 1
      }
    }
    h
  }

  /**
    * much faster and takes much less memory than groupBy + reduce
    */
  def reduceByKey[K, V](itr: Iterator[(K, V)], reducer: (V, V) => V): Map[K, V] = {

    val result = itr.foldLeft(mutable.Map[K, V]())(
      op = { (map, tt) =>
        val nv = map
          .get(tt._1)
          .map(v => reducer(v, tt._2))
          .getOrElse(tt._2)
        map.update(tt._1, nv)
        map
      }
    )
    result.toMap
  }

  case object RDDs {

    /**
      * much faster than reducing many rdds independently genetic algorithm depends on it
      */
    def batchReduce[T](
        rdds: Seq[RDD[T]]
    )(
        reducer: (T, T) => T
    ): Seq[T] = {

      val zippedRDD: RDD[(Int, T)] = rdds.zipWithIndex
        .map {
          case (rdd, id) =>
            rdd.keyBy(_ => id)
        }
        .reduce { (rdd1, rdd2) =>
          rdd1.zipPartitions(rdd2, preservesPartitioning = false) { (itr1, itr2) =>
            itr1 ++ itr2
          }
        }

      val reduced: Map[Int, T] = zippedRDD
        .mapPartitions { itr =>
          val reduced = SpookyUtils.reduceByKey[Int, T](itr, reducer)
          Iterator(reduced)
        }
        .reduce { (m1, m2) =>
          val rr = (m1.iterator ++ m2.iterator).toSeq.groupBy(_._1).view.mapValues(_.map(_._2).reduce(reducer)).toMap
          rr
        }
      reduced.sortBy(_._1).values.toSeq
    }
  }

  implicit def asCommonUtils(v: this.type): CommonUtils.type = CommonUtils
}
