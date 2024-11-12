package com.tribbloids.spookystuff.commons

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

  def liftCamelCase(str: String): String = str.head.toUpper.toString + str.substring(1)
  def toCamelCase(str: String): String = str.head.toLower.toString + str.substring(1)

  def indent(text: String, str: String = "\t"): String = {
    text.split('\n').filter(_.nonEmpty).map(str + _).mkString("\n")
  }

  def isSerializable(v: Class[?]): Boolean = {

    classOf[java.io.Serializable].isAssignableFrom(v) ||
    v.isPrimitive ||
    v.isArray
  }
}
