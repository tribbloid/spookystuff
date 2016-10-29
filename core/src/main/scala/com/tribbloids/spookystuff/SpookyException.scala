package com.tribbloids.spookystuff

/**
  * Created by peng on 9/11/14.
  * doesn't have to catch it every time
  */
class SpookyException (
                        val message: String = "",
                        val cause: Throwable = null
                      ) extends RuntimeException(message, cause) {

  override def getMessage: String = if (cause == null) this.message
  else s"${this.message}\nCaused by: ${this.getCause}"
}

class ActionException(
                       override val message: String = "",
                       override val cause: Throwable = null
                     ) extends SpookyException(message, cause) {

}

class PyException(
                       code: String,
                       output: String,
                       override val cause: Throwable = null
                     ) extends SpookyException(
  {
    s"""
       |Error interpreting:
       |$code
       |---------------------------------------
       |$output
     """.stripMargin
  },
  cause
)

class PyInterpreterException(
                              code: String,
                              output: String
                            ) extends PyException(code, output, null)

class RetryingException(
                         override val message: String = "",
                         override val cause: Throwable = null
                       ) extends ActionException(message, cause)

class DFSReadException(
                        override val message: String = "",
                        override val cause: Throwable = null
                      ) extends SpookyException(message, cause)

class DFSWriteException(
                         override val message: String = "",
                         override val cause: Throwable = null
                       ) extends SpookyException(message, cause)

//TODO: cause confusion! replace with IllegalArgumentException or use mixin
class QueryException(
                      override val message: String = "",
                      override val cause: Throwable = null
                    ) extends SpookyException(message, cause)

class BrowserDeploymentException(
                                  override val message: String = "",
                                  override val cause: Throwable = null
                                ) extends SpookyException(message, cause)
