package org.tribbloid.spookystuff

/**
 * Created by peng on 9/11/14.
 * doesn't have to catch it every time
 */
abstract class SpookyException (
                                 val message: String = "",
                                 val cause: Throwable = null
                                 )
  extends RuntimeException(message, cause) {
}
