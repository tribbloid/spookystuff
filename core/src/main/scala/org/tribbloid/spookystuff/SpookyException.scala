package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.actions.{Screenshot, Action}
import org.tribbloid.spookystuff.pages.Page

/**
 * Created by peng on 9/11/14.
 * doesn't have to catch it every time
 */
class SpookyException (
                        val message: String = "",
                        val cause: Throwable = null
                        )
  extends RuntimeException(message, cause) {

  override def getMessage: String = if (cause == null) this.message
    else s"${this.message}\nCaused by: ${this.getCause}"
}

class ActionException(
                       override val message: String = "",
                       override val cause: Throwable = null
                       ) extends SpookyException(message, cause) {

  //TODO: aggregate error handling here
//  def this(
//            action: Action,
//            snapshot: Option[Page],
//            screenshot: Option[Page],
//            cause: Throwable
//            ) {
//
//  }

}

class DFSReadException(
                        override val message: String = "",
                        override val cause: Throwable = null
                        ) extends SpookyException(message, cause)

class DFSWriteException(
                         override val message: String = "",
                         override val cause: Throwable = null
                         ) extends SpookyException(message, cause)

class QueryException(
                      override val message: String = "",
                      override val cause: Throwable = null
                      ) extends SpookyException(message, cause)

//class UnsupportedContentTypeException(
//                                        override val message: String = "",
//                                        override val cause: Throwable = null
//                                   ``     ) extends SpookyExc```ion(message, cause)