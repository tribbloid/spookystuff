package com.tribbloids.spookystuff.utils

import org.slf4j.LoggerFactory
import com.tribbloids.spookystuff.Const

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Random, Success, Try}

/**
 * Created by peng on 06/08/14.
 */
object Utils {

//  val logger = LoggerFactory.getLogger(this.getClass)

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) =>
        x
      case Failure(e) if n > 1 =>
        val logger = LoggerFactory.getLogger(this.getClass)
        logger.warn(s"Retrying locally on ${e.getClass.getSimpleName}... ${n-1} time(s) left")
        logger.info("\t\\-->", e)
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

  @transient lazy val random = new Random()

  def uriConcat(parts: String*): String = {
    var result = ""

    for (part <- parts) {
      result += uriSlash(part)
    }
    result.substring(0, result.length-1)
  }

  def uriSlash(part: String): String = {
    if (part.endsWith("/")) part
    else part+"/"
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
    var result = name.replaceAll("[ ]","_").replaceAll("""[^0-9a-zA-Z!_.*'()-]+""","*") //TODO: * is a metacharacter! why it is used as a replace string???

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
  def canonizeColumnName(name: String): String = name.replaceAllLiterally(".", "_")

  def toJson(obj: AnyRef, beautiful: Boolean = false): String = {

    import org.json4s.jackson.Serialization

    if (beautiful) Serialization.writePretty(obj)(Const.jsonFormats)
    else Serialization.write(obj)(Const.jsonFormats)
  }

  def encapsulateAsIterable(obj: Any): Iterable[Any] = obj match {
    case v: TraversableOnce[_] => v.toIterable
    case v: Array[_] => v
    case v: Any => Iterable(v)
    case _ => Nil
  }
}