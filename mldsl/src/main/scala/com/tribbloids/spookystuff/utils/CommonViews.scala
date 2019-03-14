package com.tribbloids.spookystuff.utils

import java.security.PrivilegedAction

import scala.collection.GenTraversableOnce
import scala.reflect.ClassTag
import scala.util.matching.Regex
import scala.util.matching.Regex.Match

abstract class CommonViews {

  implicit class StringView(str: String) {

    def :/(other: String): String = CommonUtils./:/(str, other)
    def \\(other: String): String = CommonUtils.\\\(str, other)

    def interpolate(delimiter: String)(
        replace: String => String
    ): String = {

      if (str == null || str.isEmpty) return str

      val specialChars = "(?=[]\\[+$&|!(){}^\"~*?:\\\\-])"
      val escaped = delimiter.replaceAll(specialChars, "\\\\")
      val regex = ("(?<!" + escaped + ")" + escaped + "\\{[^\\{\\}\r\n]*\\}").r

      val result = replaceAllNonRecursively(regex, str, m => {
        val original = m.group(0)
        val key = original.substring(2, original.length - 1)

        val replacement = replace(key)
        replacement
      })
      result
    }
  }

  def replaceAllNonRecursively(regex: Regex, target: CharSequence, replacer: Match => String): String = {

    val matches = regex.findAllMatchIn(target).toList

    val buffer = new StringBuilder()

    var previousEnd: Int = 0

    for (m <- matches) {
      val replacement = replacer(m)
      buffer.append(target.subSequence(previousEnd, m.start))
      buffer.append(replacement)
      previousEnd = m.end
    }

    buffer.toString()
  }

  implicit class Function2PrivilegedAction[T](f: => T) extends PrivilegedAction[T] {
    override def run(): T = {
      f
    }
  }

  implicit class GenTraversableOnceView[A](self: GenTraversableOnce[A])(implicit ctg: ClassTag[A]) {

    def longSize: Long = {
      var result = 0L
      for (x <- self) result += 1
      result
    }
  }
}

object CommonViews extends CommonViews
