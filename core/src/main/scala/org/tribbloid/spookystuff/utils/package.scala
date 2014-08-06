package org.tribbloid.spookystuff

import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

/**
 * Created by peng on 06/08/14.
 */
package object utils {

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int = Conf.localRetry)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }

  def withDeadline[T](n: Int)(fn: => T): T = {
    val future = Future { fn }

    return Await.result(future, n seconds)
  }

}
