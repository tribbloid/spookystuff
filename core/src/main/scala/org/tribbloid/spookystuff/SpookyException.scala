package org.tribbloid.spookystuff

/**
 * Created by peng on 9/11/14.
 * doesn't have to catch it every time
 */
class SpookyException (
                        val message: String = "",
                        val cause: Throwable = null
                        )
  extends RuntimeException(message, cause) {

  override def getMessage: String = s"${this.message}\nCaused by: ${this.getCause}"
}

class ActionException(
                       override val message: String = "",
                       override val cause: Throwable = null
                       ) extends SpookyException(message, cause)

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