package org.tribbloid.spookystuff.utils

import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.entity.PageRow

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Random, Success, Try}

/**
 * Created by peng on 06/08/14.
 */
object Utils {

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) =>
        x
      case Failure(e) if n > 1 =>
        LoggerFactory.getLogger(this.getClass).warn(s"Retrying... ${n-1} times left", e)
        retry(n - 1)(fn)
      case Failure(e) =>
        throw e
    }
  }

  def withDeadline[T](n: Duration)(fn: => T): T = {
    val future = Future { fn }

    Await.result(future, n)
  }

  //  def retryWithDeadline[T](n: Int, t: Duration)(fn: => T): T = retry(n){withDeadline(t){fn}}

  lazy val random = new Random()

  def urlConcat(parts: String*): String = {
    var result = ""

    for (part <- parts) {
      if (part.endsWith("/")) result += part
      else result += part+"/"
    }
    result.substring(0, result.length-1)
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
    var result = name.replaceAll("[ ]","_").replaceAll("""[^0-9a-zA-Z!_.*'()-]+""","*")

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

  def toJson(obj: AnyRef, beautiful: Boolean = false): String = {

    import org.json4s.jackson.Serialization

    if (beautiful) Serialization.writePretty(obj)(DefaultFormats)
    else Serialization.write(obj)(DefaultFormats)
  }

  //TODO: reverse the direction of look-up, if a '#{...}' has no corresponding indexKey in the map, throws an exception
  def interpolateFromMap[T](
                             str: String,
                             map: Map[String,T],
                             delimiter: String = Const.keyDelimiter
                             ): Option[String] = {
    if (str == null) return None
    if (str.isEmpty) return Some(str)

    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    val result = regex.replaceAllIn(str,m => {
      val original = m.group(0)
      val key = original.substring(2, original.size-1)
      map.get(key) match {
        case Some(v) => v.toString
        case None => return None
      }
    })

    Some(result)
  }

  def interpolate(
                   str: String,
                   row: PageRow,
                   delimiter: String = Const.keyDelimiter
                   ): Option[String] = {
    if (str == null) return None
    if (str.isEmpty) return Some(str)

    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    val result = regex.replaceAllIn(str,m => {
      val original = m.group(0)
      val key = original.substring(2, original.size-1)
      row.get(key) match {
        case Some(v) => v.toString
        case None => return None
      }
    })

    Some(result)
  }

  implicit def mapFunctions[K](map: Map[K, _]): MapFunctions[K] = new MapFunctions[K](map)
}
