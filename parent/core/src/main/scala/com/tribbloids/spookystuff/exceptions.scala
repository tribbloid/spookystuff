package com.tribbloids.spookystuff

import java.io.IOException

import com.tribbloids.spookystuff.commons.TreeException

trait SpookyException extends Exception with TreeException {

  def cause: Throwable = null

  override def getCause: Throwable = cause
}

class ActionException(
    override val getMessage_simple: String = "",
    override val cause: Throwable = null
) extends SpookyException {}

class PyException(
    val code: String,
    val output: String,
    override val cause: Throwable = null,
    val historyCodeOpt: Option[String] = None
) extends SpookyException {

  override def getMessage_simple: String =
    s"""
       |${historyCodeOpt.map(v => "\t### History ###\n" + v).getOrElse("")}
       |
       |${"\t"}### Error interpreting: ###
       |
       |$code
       |================== TRACEBACK / ERROR ==================
       |$output
     """.stripMargin.trim
}

case class PyInterpretationException(
    override val code: String,
    override val output: String,
    override val cause: Throwable = null,
    override val historyCodeOpt: Option[String] = None
) extends PyException(code, output, cause, historyCodeOpt)

class RetryingException(
    override val getMessage_simple: String = "",
    override val cause: Throwable = null
) extends ActionException(getMessage_simple, cause)

class DFSReadException(
    override val getMessage_simple: String = "",
    override val cause: Throwable = null
) extends IOException(getMessage_simple, cause)
    with SpookyException

class DFSWriteException(
    override val getMessage_simple: String = "",
    override val cause: Throwable = null
) extends IOException(getMessage_simple, cause)
    with SpookyException

//TODO: cause confusion! replace with IllegalArgumentException or use mixin
class QueryException(
    override val getMessage_simple: String = "",
    override val cause: Throwable = null
) extends SpookyException

class BrowserDeploymentException(
    override val getMessage_simple: String = "",
    override val cause: Throwable = null
) extends SpookyException
