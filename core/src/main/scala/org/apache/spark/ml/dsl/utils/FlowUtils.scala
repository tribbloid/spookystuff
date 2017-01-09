package org.apache.spark.ml.dsl.utils

object FlowUtils {

  def cartesianProductSet[T](xss: Seq[Set[T]]): Set[List[T]] = xss match {
    case Nil => Set(Nil)
    case h :: t => for(
      xh <- h;
      xt <- cartesianProductSet(t)
    )
      yield xh :: xt
  }

  def cartesianProductList[T](xss: Seq[Seq[T]]): Seq[List[T]] = xss match {
    case Nil => List(Nil)
    case h :: t => for(
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

  def getBreakpointInfo(
                         filterInitializer: Boolean = true,
                         filterLazyRelay: Boolean = true,
                         filterDefaultRelay: Boolean = true
                       ): Array[StackTraceElement] = {
    val stackTraceElements: Array[StackTraceElement] = Thread.currentThread().getStackTrace
    var effectiveElements = stackTraceElements

    if (filterInitializer) effectiveElements = effectiveElements.filter(v => !(v.getMethodName == INIT))
    if (filterLazyRelay) effectiveElements = effectiveElements.filter(v => !v.getMethodName.endsWith(LZYCOMPUTE))

    effectiveElements
      .slice(2, Int.MaxValue)
  }

  def getCallerMethodName(stackDepth: Int = 3): String = {
    val bp = FlowUtils.getBreakpointInfo().apply(stackDepth)
    assert(!bp.isNativeMethod) //can only use default value in def & lazy val blocks
    bp.getMethodName
  }

  def liftCamelCase(str: String) = str.head.toUpper.toString + str.substring(1)
  def toCamelCase(str: String) = str.head.toLower.toString + str.substring(1)

  class ThreadLocal[A](init: => A) extends java.lang.ThreadLocal[A] with (() => A) {
    override def initialValue = init
    def apply = get
  }
}