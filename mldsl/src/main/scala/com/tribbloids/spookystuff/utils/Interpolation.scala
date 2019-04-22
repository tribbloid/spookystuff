package com.tribbloids.spookystuff.utils

import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.matching.Regex.Match

case class Interpolation(delimiter: String) {

  import Interpolation._

  val escaped: String = delimiter.replaceAll(specialChars, "\\\\")
  val regex: Regex = ("(?<!" + escaped + ")" + escaped + "\\{[^\\{\\}\r\n]*\\}").r

  case class Compile(str: String)(replace: String => String) {

    def useRegex(): String = {
      if (str == null || str.isEmpty) return str

      val result = replaceAllNonRecursively(regex, str, m => {
        val original = m.group(0)
        val key = original.substring(2, original.length - 1)

        val replacement = replace(key)
        replacement
      })
      result
    }

    def run(): String = useRegex()
  }

  def apply(str: String)(replace: String => String): String = Compile(str)(replace).run()
}

object Interpolation {

  implicit def fromDelimiter(v: String): Interpolation = Interpolation(v)

  def `$`: Interpolation = "$"
  def `'`: Interpolation = "'"

  val specialChars = "(?=[]\\[+$&|!(){}^\"~*?:\\\\-])"

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

    buffer.append(target.subSequence(previousEnd, target.length()))

    buffer.toString()
  }
}
