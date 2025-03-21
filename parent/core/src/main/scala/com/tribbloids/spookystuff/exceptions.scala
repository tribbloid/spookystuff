package com.tribbloids.spookystuff

import java.io.IOException

import com.tribbloids.spookystuff.commons.TreeException

trait SpookyException extends TreeException {}

class ActionException(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends SpookyException {}

class PyException(
    val code: String,
    val output: String,
    override val getCause: Throwable = null,
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
    override val getCause: Throwable = null,
    override val historyCodeOpt: Option[String] = None
) extends PyException(code, output, getCause, historyCodeOpt)

class RetryingException(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends ActionException(getMessage_simple, getCause)

class DFSReadException(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends IOException(getMessage_simple, getCause)
    with SpookyException

class DFSWriteException(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends IOException(getMessage_simple, getCause)
    with SpookyException

//TODO: cause confusion! replace with IllegalArgumentException or use mixin
class QueryException(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends SpookyException

class BrowserDeploymentException(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends SpookyException
