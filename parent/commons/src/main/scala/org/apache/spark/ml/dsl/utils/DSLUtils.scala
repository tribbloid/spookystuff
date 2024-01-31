package org.apache.spark.ml.dsl.utils

object DSLUtils {

  def cartesianProductSet[T](xss: Seq[Set[T]]): Set[List[T]] = xss match {
    case Nil => Set(Nil)
    case h :: t =>
      for (
        xh <- h;
        xt <- cartesianProductSet(t)
      )
        yield xh :: xt
  }

  def cartesianProductList[T](xss: Seq[Seq[T]]): Seq[List[T]] = xss match {
    case Nil => List(Nil)
    case h :: t =>
      for (
        xh <- h;
        xt <- cartesianProductList(t)
      )
        yield xh :: xt
  }

  //  def jValue(obj: Any)(implicit formats: Formats = DefaultFormats): JValue = Extraction.decompose(obj)
  //  def compactJSON(obj: Any)(implicit formats: Formats = DefaultFormats) = compact(render(jValue(obj)))
  //  def prettyJSON(obj: Any)(implicit formats: Formats = DefaultFormats) = pretty(render(jValue(obj)))
  //
  //  def toJSON(obj: Any, pretty: Boolean = false)(implicit formats: Formats = DefaultFormats): String = {
  //    if (pretty) compactJSON(obj)
  //    else prettyJSON(obj)
  //  }

  private lazy val LZYCOMPUTE = "$lzycompute"
  private lazy val INIT = "<init>"

  def stackTracesShowStr(
      vs: Array[StackTraceElement],
      maxDepth: Int = 1
  ): String = {
    vs.slice(0, maxDepth)
      .mkString("\n\t< ")
  }

  final private val breakpointInfoBlacklist = {
    Seq(
      this.getClass.getCanonicalName,
      classOf[Thread].getCanonicalName
    ).map(_.stripSuffix("$"))
  }
  private def breakpointInfoFilter(vs: Array[StackTraceElement]) = {
    vs.filterNot { v =>
      val className = v.getClassName
      val outerClassName = className.split('$').head
      outerClassName.startsWith("scala") ||
      breakpointInfoBlacklist.contains(outerClassName)
    }
  }

  def getBreakpointInfo(
      filterAnon: Boolean = true,
      filterInitializer: Boolean = true,
      filterLazyCompute: Boolean = true
  ): Array[StackTraceElement] = {
    val stackTraceElements: Array[StackTraceElement] = Thread.currentThread().getStackTrace
    var effectiveElements = breakpointInfoFilter(stackTraceElements)

    if (filterAnon) effectiveElements = effectiveElements.filter(v => !v.getMethodName.contains('$'))
    // TODO: this impl needs improvement

    if (filterInitializer) effectiveElements = effectiveElements.filter(v => !(v.getMethodName == INIT))
    if (filterLazyCompute) effectiveElements = effectiveElements.filter(v => !v.getMethodName.endsWith(LZYCOMPUTE))

    effectiveElements
  }

  def liftCamelCase(str: String): String = str.head.toUpper.toString + str.substring(1)
  def toCamelCase(str: String): String = str.head.toLower.toString + str.substring(1)

  def indent(text: String, str: String = "\t"): String = {
    text.split('\n').filter(_.nonEmpty).map(str + _).mkString("\n")
  }

//  lazy val defaultJavaSerializer: JavaSerializer = {
//    val conf = new SparkConf()
//    new JavaSerializer(conf)
//  }
//
//  lazy val defaultKryoSerializer: KryoSerializer = {
//    val conf = new SparkConf()
//    new KryoSerializer(conf)
//  }

  def isSerializable(v: Class[_]): Boolean = {

    classOf[java.io.Serializable].isAssignableFrom(v) ||
    v.isPrimitive ||
    v.isArray
  }
}
