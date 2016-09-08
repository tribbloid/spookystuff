package com.tribbloids.spookystuff.utils

import java.io.{File, InputStream}
import java.net._
import java.nio.file._
import java.util.zip.ZipInputStream

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.dsl.ReflectionUtils
import org.apache.spark.sql.catalyst.ScalaReflection
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.xml.PrettyPrinter

object SpookyUtils {

  import ImplicitUtils._
  import ScalaReflection.universe._

  def numCores = {
    val result = Runtime.getRuntime.availableProcessors()
    assert(result > 0)
    result
  }

  val xmlPrinter = new PrettyPrinter(Int.MaxValue, 2)
  //  val logger = LoggerFactory.getLogger(this.getClass)

  val \\\ = File.separator

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) =>
        x
      case util.Failure(e) if n > 1 =>
        val logger = LoggerFactory.getLogger(this.getClass)
        logger.warn(s"Retrying locally on ${e.getClass.getSimpleName}... ${n-1} time(s) left")
        logger.info("\t\\-->", e)
        retry(n - 1)(fn)
      case util.Failure(e) =>
        throw e
    }
  }

  @annotation.tailrec
  def retryExplicitly[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) =>
        x
      case util.Failure(e) if n > 1 =>
        println(s"Retrying locally on ${e.getClass.getSimpleName}... ${n-1} time(s) left")
        println("\t\\-->", e)
        retryExplicitly(n - 1)(fn)
      case util.Failure(e) =>
        throw e
    }
  }

  def withDeadline[T](n: Duration)(fn: => T): T = {
    val future = Future {
      fn
    }

    //TODO: this doesn't terminate the future upon timeout exception! need a better pattern.
    Await.result(future, n)
  }

  //  def retryWithDeadline[T](n: Int, t: Duration)(fn: => T): T = retry(n){withDeadline(t){fn}}

  @transient lazy val random = new util.Random()

  def pathConcat(parts: String*): String = {
    parts.reduceOption(_ :/ _).getOrElse("")
  }

  /*
For Amazon S3:

The following character sets are generally safe for use in key names:

Alphanumeric characters [0-9a-zA-Z]

Special characters !, -, _, ., *, ', (, and )

Characters That Might Require Special Handling

The following characters in a key name may require additional code handling and will likely need to be URL encoded or referenced as HEX. Some of these are non-printable characters and your browser may not handle them, which will also require special handling:

Ampersand ("&")
Dollar ("$")
ASCII character ranges 00–1F hex (0–31 decimal) and 7F (127 decimal.)
'At' symbol ("@")
Equals ("=")
Semicolon (";")
Colon (":")
Plus ("+")
Space – Significant sequences of spaces may be lost in some uses (especially multiple spaces)
Comma (",")
Question mark ("?")

Characters to Avoid

You should avoid the following characters in a key name because of significant special handling for consistency across all applications.

Backslash ("\")
Left curly brace ("{")
Non-printable ASCII characters (128–255 decimal characters)
Caret ("^")
Right curly brace ("}")
Percent character ("%")
Grave accent / back tick ("`")
Right square bracket ("]")
Quotation marks
'Greater Than' symbol (">")
Left square bracket ("[")
Tilde ("~")
'Less Than' symbol ("<")
'Pound' character ("#")
Vertical bar / pipe ("|")

there are 12 characters with special meanings:
the backslash \,
the caret ^,
the dollar sign $,
the period or dot .,
the vertical bar or pipe symbol |,
the question mark ?,
the asterisk or star *,
the plus sign +,
the opening parenthesis (,
the closing parenthesis ),
and the opening square bracket [,
the opening curly brace {,
These special characters are often called "metacharacters".
  */
  def canonizeFileName(name: String): String = {

    var result = name.replaceAll("[ ]","_").replaceAll("""[^0-9a-zA-Z!_.*'()-]+""","_")

    if (result.length > 255) result = result.substring(0, 255)

    result
  }

  def canonizeUrn(name: String): String = {

    var result = name.replaceAll("[ ]","_").replaceAll("""[^0-9a-zA-Z!_.*'()-]+""","/")

    result = result.split("/").map{
      part => {
        if (part.length > 255) part.substring(0, 255)
        else part
      }
    }.mkString("/")

    result
  }

  // Spark SQL does not currently support column names with dots (see SPARK-2775),
  // so we'll need to post-process the inferred schema to convert dots into underscores:
  def canonizeColumnName(name: String): String = {
    name.replaceAllLiterally(".", "_")
  }

  def typedOrNone[B: ClassTag](v: Any): Option[B] = {
    val array = try {
      Array[B](v.asInstanceOf[B])
    }
    catch {
      case e: Throwable =>
        Array[B]()
    }
    array.headOption
  }

  //TODO: move to class & try @Specialized?
  def asArray[T <: Any : ClassTag](obj: Any): Array[T] = {

    obj match {
      case v: TraversableOnce[Any] => v.toArray.filterByType[T]
      case v: Array[T] => v.filterByType[T]
      case _ =>
        Array[Any](obj).filterByType[T]
    }
  }

  def asIterable[T <: Any : ClassTag](obj: Any): Iterable[T] = {

    obj match {
      case v: TraversableOnce[Any] => v.toArray.filterByType[T]
      case v: Array[T] => v.filterByType[T].toIterable
      case _ =>
        Array[Any](obj).filterByType[T]
    }
  }

  def validateLocalPath(path: String): Option[String] = {
    if (path == null) return None
    val file = new File(path)
    if (file.exists()) Some(path)
    else None
  }

  //TODO: need test, or its already superceded by try catch?
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
      case o @_ =>
        o
    }
  }

  def caseAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList

  def getCPResource(str: String): Option[URL] =
    Option(ClassLoader.getSystemClassLoader.getResource(str.stripSuffix(SpookyUtils.\\\)))
  def getCPResourceAsStream(str: String): Option[InputStream] =
    Option(ClassLoader.getSystemClassLoader.getResourceAsStream(str.stripSuffix(SpookyUtils.\\\)))

  def addCPResource(urlStr: String): Unit = {

    val url = scala.reflect.io.File(urlStr.stripSuffix("/")).toAbsolute.toURL

    assert(url.toString.startsWith("file"))

    ReflectionUtils.invoke(
      classOf[URLClassLoader],
      ClassLoader.getSystemClassLoader,
      "addURL",
      classOf[URL] -> url
    )

    assert(ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader].getURLs.contains(url))
  }

//  def nioStdCopy(srcPath: Path, dstFile: File): Any = {
//    if (!dstFile.exists()) {
//      dstFile.getParentFile.mkdirs()
//      Files.copy(
//        srcPath,
//        dstFile.toPath,
//        StandardCopyOption.COPY_ATTRIBUTES,
//        StandardCopyOption.REPLACE_EXISTING
//      )
//    }
//  }

  def universalCopy(srcPath: Path, dstPath: Path): Any = {

    Files.walkFileTree(srcPath, java.util.EnumSet.of(FileVisitOption.FOLLOW_LINKS),
      Integer.MAX_VALUE, new CopyDirectory(srcPath, dstPath))
  }
  def asynchIfNotExist[T](dst: String)(f: =>T): Option[T] = this.synchronized {
    val dstFile = new File(dst)
    if (!dstFile.exists()) {
      Some(f)
    }
    else {
      None
    }
  }

  //TODO: this is not tested on workers
  def extractResource(resource: URL, dst: String): Unit = {

    resource.getProtocol match {
      case "jar" =>
        val fullPath = resource.toString
        val splitted = fullPath.split('!')
        assert(splitted.length == 2)
        val jarPath = splitted.head
        val innerPathStr = splitted.last

        val zip = new ZipInputStream(resource.openStream())

        val fs = FileSystems.newFileSystem(new URI(fullPath), new java.util.HashMap[String, String]())
        val srcPath = fs.getPath(innerPathStr)

        universalCopy(srcPath, new File(dst).toPath)
      case _ =>
        val src = new File(resource.toURI)
        universalCopy(src.toPath, new File(dst).toPath)
    }
  }


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

  @scala.annotation.tailrec
  def unboxException[T <: Throwable: ClassTag](e: Throwable): Throwable = {
    e match {
      case ee: T =>
        unboxException[T](ee.getCause)
      case _ =>
        e
    }
  }
}