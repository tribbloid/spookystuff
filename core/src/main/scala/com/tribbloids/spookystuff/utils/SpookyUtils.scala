package com.tribbloids.spookystuff.utils

import java.io.{File, InputStream}
import java.net._
import java.nio.file._

import com.tribbloids.spookystuff.utils.NoRetry.NoRetryWrapper
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

  import ScalaReflection.universe._
  import SpookyViews._

  def qualifiedName(separator: String)(parts: String*) = {
    parts.flatMap(v => Option(v)).reduceLeftOption(addSuffix(separator, _) + _).orNull
  }
  def addSuffix(suffix: String, part: String) = {
    if (part.endsWith(suffix)) part
    else part+suffix
  }

  def /:/(parts: String*): String = qualifiedName("/")(parts: _*)
  def :/(part: String): String = addSuffix("/", part)

  def \\\(parts: String*): String = qualifiedName(File.separator)(parts: _*)
  def :\(part: String): String = addSuffix(File.separator, part)

  def numCores = {
    val result = Runtime.getRuntime.availableProcessors()
    assert(result > 0)
    result
  }

  val xmlPrinter = new PrettyPrinter(Int.MaxValue, 2)

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int, interval: Long = 0)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) =>
        x
      case util.Failure(e: NoRetryWrapper) => throw e.getCause
      case util.Failure(e) if n > 1 =>
        val logger = LoggerFactory.getLogger(this.getClass)
        logger.warn(s"Retrying locally on ${e.getClass.getSimpleName} in ${interval.toDouble/1000} second(s)... ${n-1} time(s) left")
        logger.debug("\t\\-->", e)
        Thread.sleep(interval)
        retry(n - 1, interval)(fn)
      case util.Failure(e) =>
        throw e
    }
  }

  def withDeadline[T](
                       n: Duration,
                       heartbeat: Option[Duration] = Some(10.seconds)
                       //                       name: String = FlowUtils.getBreakpointInfo().apply(2).getMethodName
                       //TODO: default name not working for inner function
                     )(fn: => T): T = {

    val future = Future {
      fn
    }

    @volatile var completed = false
    try {
      heartbeat.foreach {
        hb =>
          val printer = Future {
            val current = System.currentTimeMillis()
            while(!completed) {
              Thread.sleep(hb.toMillis)
              val elapsed = (System.currentTimeMillis() - current).millis
              val left = n.minus(elapsed)
              assert(left.toMillis > 0, "INTERNAL ERROR: heartbeat not terminated")
              LoggerFactory.getLogger(this.getClass).info(
                s"T - ${left.toMillis.toDouble/1000} second(s)"
              )
            }
          }
      }

      //TODO: this doesn't terminate the future upon timeout exception! need a better pattern.
      val result = Await.result(future, n)
      result
    }
    finally {
      completed = true
    }
  }

  //  def retryWithDeadline[T](n: Int, t: Duration)(fn: => T): T = retry(n){withDeadline(t){fn}}

  @transient lazy val random = new util.Random()

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

  object Reflection {
    def getCaseAccessorSymbols[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
      .toList

    def getCaseAccessorNames[T: TypeTag]: List[(String, Type)] = {
      getCaseAccessorSymbols[T].map {
        ss =>
          ss.name.decoded -> ss.typeSignature
      }
    }

    //the following are copied from Spark ScalaReflection
    def getConstructorParameters(cls: Class[_]): Seq[(String, Type)] = {
      val m = runtimeMirror(cls.getClassLoader)
      val classSymbol = m.staticClass(cls.getName)
      val t = classSymbol.selfType
      getConstructorParameters(t)
    }

    def getConstructorParameters(tpe: Type): Seq[(String, Type)] = {
      val formalTypeArgs = tpe.typeSymbol.asClass.typeParams
      val TypeRef(_, _, actualTypeArgs) = tpe
      val constructorSymbol = tpe.member(nme.CONSTRUCTOR)
      val params = if (constructorSymbol.isMethod) {
        constructorSymbol.asMethod.paramss
      } else {
        // Find the primary constructor, and use its parameter ordering.
        val primaryConstructorSymbol: Option[Symbol] = constructorSymbol.asTerm.alternatives.find(
          s => s.isMethod && s.asMethod.isPrimaryConstructor)
        if (primaryConstructorSymbol.isEmpty) {
          sys.error("Internal SQL error: Product object did not have a primary constructor.")
        } else {
          primaryConstructorSymbol.get.asMethod.paramss
        }
      }

      params.flatten.map { p =>
        p.name.toString -> p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
      }
    }
  }

  def getCPResource(str: String): Option[URL] =
    Option(ClassLoader.getSystemClassLoader.getResource(str.stripSuffix(File.separator)))
  def getCPResourceAsStream(str: String): Option[InputStream] =
    Option(ClassLoader.getSystemClassLoader.getResourceAsStream(str.stripSuffix(File.separator)))

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

  def stringInterpolate(str: String, delimiter: String)(
    replace: String => String
  ): String = {

    if (str.isEmpty) return str

    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    val result = regex.replaceAllIn(str,m => {
      val original = m.group(0)
      val key = original.substring(2, original.length - 1)

      val replacement = replace(key)
      replacement
    })
    result
  }
}