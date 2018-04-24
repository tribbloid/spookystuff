package com.tribbloids.spookystuff.utils

import java.security.PrivilegedAction

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

      val result = regex.replaceAllIn(
        str,
        m => {
          val original = m.group(0)
          val key = original.substring(2, original.length - 1)

          val replacement = replace(key)
          replacement
        })
      result
    }
  }

  implicit class Function2PrivilegedAction[T](f: => T) extends PrivilegedAction[T] {
    override def run(): T = {
      f
    }
  }
}

object CommonViews extends CommonViews