package org.tribbloid.spookystuff

import org.json4s.DefaultFormats

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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
      case _ if n > 1 =>
        retry(n - 1)(fn)
      case Failure(e) =>
        throw e
    }
  }

  def withDeadline[T](n: Duration)(fn: => T): T = {
    val future = Future { fn }

    Await.result(future, n)
  }

  def retryWithDeadline[T](n: Int, t: Duration)(fn: => T): T = retry(n){withDeadline(t){fn}}

  lazy val random = new Random()

  def urlConcat(parts: String*): String = {
    var result = ""

    for (part <- parts) {
      if (part.endsWith("/")) result += part
      else result += part+"/"
    }
    result
  }

  def canonizeFileName(name: String): String = {
    var result = name.replaceAll("[ ]","_").replaceAll("[,&\\?]+","|").replaceAll("[:\\\\/]+", "*")

    if (result.length > 255) result = result.substring(0, 255)

    result
  }

  def canonizeUrn(name: String): String = {

    var result = name.replaceAll("[ ]","_").replaceAll("[:,&\\?\\\\]+","/")

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
}
