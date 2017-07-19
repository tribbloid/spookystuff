package com.tribbloids.spookystuff.utils

import java.io.{File, InputStream}
import java.net._
import java.nio.file.{Files, _}

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkEnv
import org.apache.spark.ml.dsl.ReflectionUtils
import org.apache.spark.ml.dsl.utils.FlowUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}
import scala.xml.PrettyPrinter

object SpookyUtils {

  import SpookyViews._

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

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
  def retry[T](
                n: Int,
                interval: Long = 0,
                silent: Boolean = false,
                callerStr: String = null
              )(fn: => T): T = {
    var _callerStr = callerStr
    //TODO: this should be exposed as utility.
    if (callerStr == null)
      _callerStr = FlowUtils.stackTracesShowStr(
        FlowUtils.getBreakpointInfo()
          .slice(1, Int.MaxValue)
        //          .filterNot(_.getClassName == this.getClass.getCanonicalName)
      )
    Try { fn } match {
      case Success(x) =>
        x
      case Failure(e: NoRetry.Wrapper) =>
        throw e.getCause
      case Failure(e) if n > 1 =>
        if (!(silent || e.isInstanceOf[SilentRetry.Wrapper])) {
          val logger = LoggerFactory.getLogger(this.getClass)
          logger.warn(
            s"Retrying locally on ${e.getClass.getSimpleName} in ${interval.toDouble/1000} second(s)... ${n-1} time(s) left" +
              "\t@ " + _callerStr +
              "\n" + e.getMessage
          )
          logger.debug("\t\\-->", e)
        }
        Thread.sleep(interval)
        retry(n - 1, interval, callerStr = _callerStr)(fn)
      case Failure(e) =>
        throw e
    }
  }

  def withDeadline[T](
                       n: Duration,
                       heartbeatOpt: Option[Duration] = Some(10.seconds)
                     )(
                       fn: =>T,
                       heartbeatFn: Option[Int => Unit] = None
                     ): T = {

    val breakpointStr: String = FlowUtils.stackTracesShowStr(
      FlowUtils.getBreakpointInfo()
        .slice(1, Int.MaxValue)
      //        .filterNot(_.getClassName == this.getClass.getCanonicalName)
    )
    val startTime = System.currentTimeMillis()

    var thread: Thread = null
    val future = Future {
      thread = Thread.currentThread()
      fn
    }

    val nMillis = n.toMillis
    val terminateAt = startTime + nMillis

    val effectiveHeartbeatFn: (Int) => Unit = heartbeatFn.getOrElse {
      i =>
        val remainMillis = terminateAt - System.currentTimeMillis()
        LoggerFactory.getLogger(this.getClass).info(
          s"T - ${remainMillis.toDouble/1000} second(s)" +
            "\t@ " + breakpointStr
        )
    }

    //TODO: this doesn't terminate the future upon timeout exception! need a better pattern.
    heartbeatOpt match {
      case None =>
        try {
          Await.result(future, n)
        }
        catch {
          case e: TimeoutException =>
            LoggerFactory.getLogger(this.getClass).debug("TIMEOUT!!!!")
            throw e
        }
      case Some(heartbeat) =>
        val heartbeatMillis = heartbeat.toMillis
        for (i <- 0 to (nMillis / heartbeatMillis).toInt) {
          val remainMillis = Math.max(terminateAt - System.currentTimeMillis(), 0L)
          effectiveHeartbeatFn(i)
          val epochMillis = Math.min(heartbeatMillis, remainMillis)
          try {
            val result =  Await.result(future, epochMillis.milliseconds)
            return result
          }
          catch {
            case e: TimeoutException =>
              if (heartbeatMillis >= remainMillis) {
                Option(thread).foreach(_.interrupt())
                LoggerFactory.getLogger(this.getClass).debug("TIMEOUT!!!!")
                throw e
              }
          }
        }
        throw new UnknownError("IMPOSSIBLE")
    }
  }

  //  def retryWithDeadline[T](n: Int, t: Duration)(fn: => T): T = retry(n){withDeadline(t){fn}}

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

  //TODO: simply by using a common relay that different type representation can be cast into
  /**
    * all implementation has to be synchronized and preferrably not executed concurrently to preserve efficiency.
    */
  object Reflection extends ReflectionLock {

    import org.apache.spark.sql.catalyst.ScalaReflection.universe._

    def getCaseAccessorSymbols(tt: ScalaType[_]): List[MethodSymbol] = locked{
      val accessors = tt.asType.members
        .toList
        .reverse
        .flatMap(filterCaseAccessors)
      accessors
    }

    def filterCaseAccessors(s: Symbol): Seq[MethodSymbol] = {
      s match {
        case m: MethodSymbol if m.isCaseAccessor =>
          Seq(m)
        case t: TermSymbol =>
          t.allOverriddenSymbols.flatMap(filterCaseAccessors)
        case _ =>
          Nil
      }
    }

    def getCaseAccessorFields(tt: ScalaType[_]): List[(String, Type)] = {
      getCaseAccessorSymbols(tt).map {
        ss =>
          ss.name.decoded -> ss.typeSignature
      }
    }

    def getConstructorParameters(tt: ScalaType[_]): Seq[(String, Type)] = locked{
      val formalTypeArgs = tt.asType.typeSymbol.asClass.typeParams
      val TypeRef(_, _, actualTypeArgs) = tt.asType
      val constructorSymbol = tt.asType.member(nme.CONSTRUCTOR)
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

    def getCaseAccessorMap(v: Product): List[(String, Any)] = {
      val tt = ScalaType.fromClass(v.getClass)
      val ks = getCaseAccessorFields(tt).map(_._1)
      val vs = v.productIterator.toList
      assert (ks.size == vs.size)
      ks.zip(vs)
    }

    //    def newCase[A]()(implicit t: ClassTag[A]): A = {
    //      val cm = rootMirror
    //      val clazz = cm classSymbol t.runtimeClass
    //      val modul = clazz.companionSymbol.asModule
    //      val im = cm reflect (cm reflectModule modul).instance
    //      ReflectionUtils.invokeStatic(clazz)
    //      defaut[A](im, "apply")
    //    }
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

  def resilientCopy(src: Path, dst: Path, options: Array[CopyOption]): Unit ={
    retry(5, 1000){

      val pathsStr = src + " => " + dst

      if (Files.isDirectory(src)) {
        try {
          Files.copy(src, dst, options: _*)
          //TODO: how to flush dst?
        }
        catch {
          case e: DirectoryNotEmptyException =>
        }

        val dstFile = new File(dst.toString)
        assert(dstFile.isDirectory)

        LoggerFactory.getLogger(this.getClass).debug(pathsStr + " no need to copy directory")
      }
      else {
        Files.copy(src, dst, options: _*) //this will either 1. copy file if src is a file. 2. create empty dir if src is a dir.
        //TODO: how to flush dst?

        //assert(Files.exists(dst))
        //NIO copy should use non-NIO for validation to eliminate stream caching
        val dstContent = LocalResolver.input(dst.toString){
          fis =>
            IOUtils.toByteArray(fis)
        }
        //      assert(srcContent.length == dstContent.length, pathsStr + " copy failed")
        LoggerFactory.getLogger(this.getClass).debug(pathsStr + s" ${dstContent.length} byte(s) copied")
      }
    }
  }

  def treeCopy(srcPath: Path, dstPath: Path): Any = {

    Files.walkFileTree (
      srcPath,
      java.util.EnumSet.of(FileVisitOption.FOLLOW_LINKS),
      Integer.MAX_VALUE,
      new CopyDirectoryFileVisitor(srcPath, dstPath)
    )
  }
  def ifFileNotExist[T](dst: String)(f: =>T): Option[T] = this.synchronized {
    val dstFile = new File (dst)
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
        val split = fullPath.split('!')
        assert(split.length == 2)
        val jarPath = split.head
        val innerPathStr = split.last

        val fs = FileSystems.newFileSystem(new URI(jarPath), new java.util.HashMap[String, String]())
        try {
          val srcPath = fs.getPath(innerPathStr)

          treeCopy(srcPath, new File(dst).toPath)
        }
        finally {
          fs.close()
        }
      case _ =>
        val src = new File(resource.toURI)
        treeCopy(src.toPath, new File(dst).toPath)
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

  def randomSuffix = Math.abs(Random.nextLong())

  def randomChars: String = {
    val len = Random.nextInt(128)
    Random.nextString(len)
  }

  /**
    * From doc of org.apache.spark.scheduler.TaskLocation
    * Create a TaskLocation from a string returned by getPreferredLocations.
    * These strings have the form executor_[hostname]_[executorid], [hostname], or
    * hdfs_cache_[hostname], depending on whether the location is cached.
    * def apply(str: String): TaskLocation
    * ...
    * Not sure if it will change in future Spark releases
    */
  def getTaskLocationStr: String = {

    val bmID = getBlockManagerID
    val hostPort = bmID.hostPort

    if (org.apache.spark.SPARK_VERSION.substring(0, 3).toDouble >= 1.6) {
      val executorID = bmID.executorId
      s"executor_${hostPort}_$executorID"
    }
    else {
      hostPort
    }
  }

  def getBlockManagerID: BlockManagerId = {
    val bmID = SparkEnv.get.blockManager.blockManagerId
    bmID
  }

  def tryParseBoolean(str: =>String): Try[Boolean] = {
    Try{str}.flatMap {
      v =>
        v.toLowerCase match {
          case "true" | "1" | "" => Success(true)
          case "false" | "0" | "-1" => Success(false)
          case _ => Failure(
            new UnsupportedOperationException(
              s"$v is not a boolean value"
            )
          )
        }
    }
  }

  /**
    * much faster and takes much less memory than groupBy + reduce
    */
  def reduceByKey[K, V](itr: Iterator[(K, V)], reducer: (V, V) => V): Map[K, V] = {

    val result = itr.foldLeft(mutable.Map[K, V]())(
      op = {
        (map, tt) =>
          val nv = map.get(tt._1)
            .map(v => reducer(v, tt._2))
            .getOrElse(tt._2)
          map.update(tt._1, nv)
          map
      }
    )
    result.toMap
  }

  object RDDs {

    /**
      * much faster than reducing many rdds independently
      * genetic algorithm depends on it
      */
    def batchReduce[T](
                        rdds: Seq[RDD[T]]
                      )(
                        reducer: (T, T) => T
                      ): Seq[T] = {

      val zippedRDD: RDD[(Int, T)] = rdds
        .zipWithIndex
        .map {
          case (rdd, id) =>
            rdd.keyBy(_ => id)
        }
        .reduce {
          (rdd1, rdd2) =>
            rdd1.zipPartitions(rdd2, preservesPartitioning = false) {
              (itr1, itr2) =>
                itr1 ++ itr2
            }
        }

      val reduced: Map[Int, T] = zippedRDD
        .mapPartitions {
          itr =>
            val reduced = reduceByKey[Int, T](itr, reducer)
            Iterator(reduced)
        }
        .reduce {
          (m1, m2) =>
            val rr = (m1.iterator ++ m2.iterator).toSeq.groupBy(_._1).mapValues(_.map(_._2).reduce(reducer))
            rr
        }
      reduced.sortBy(_._1).values.toSeq
    }
  }
}