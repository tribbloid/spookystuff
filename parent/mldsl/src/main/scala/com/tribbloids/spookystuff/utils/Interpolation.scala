package com.tribbloids.spookystuff.utils

import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.matching.Regex.Match

case class Interpolation(delimiter: String) {

  import Interpolation._

  val delimited: String = delimiter.replaceAll(specialChars, "\\\\")

  val regex: Regex = (delimited + "\\{[^\\{\\}\r\n]*\\}").r

  val escapedRegex: Regex = (delimited + delimited).r

  case class Compile(str: String)(replace: String => String) {

    def useRegex(): String = {
      if (str == null || str.isEmpty) return str

      val escaped = escapedRegex.findAllMatchIn(str).toList
      val escapedSection = escaped.flatMap(v => v.start until v.end).toSet

      val replaced1 = replaceAllNonRecursively(regex, str) { m =>
        if (escapedSection.contains(m.start)) {

          m.toString
        } else {

          val original = m.group(0)
          val key = original.substring(2, original.length - 1)

          val replacement = replace(key)
          replacement
        }
      }

      val replaced2 = replaceAllNonRecursively(escapedRegex, replaced1) { _ =>
        delimiter
      }

      replaced2
    }

    def run(): String = useRegex()
  }

  def apply(str: String)(replace: String => String): String = Compile(str)(replace).run()
}

object Interpolation {

  implicit def fromDelimiter(v: String): Interpolation = Interpolation(v)

  def `$`: Interpolation = "$"
  def `'`: Interpolation = "'"

  val specialChars: String = "(?=[]\\[+$&|!(){}^\"~*?:\\\\-])"

  def replaceAllNonRecursively(regex: Regex, target: CharSequence)(replacer: Match => String): String = {

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
