package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.SpookyException.HasCoreDump
import com.tribbloids.spookystuff.commons.TreeException
import com.tribbloids.spookystuff.doc.Doc

import java.io.IOException

trait SpookyException extends TreeException {}

object SpookyException {

  trait HasDoc extends SpookyException {

    val doc: Doc
  }

  trait HasCoreDump extends SpookyException {
    // core dump becomes its own type as
  }
}

class ActionException(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends SpookyException {}

class ActionExceptionWithCoreDump(
    override val getMessage_simple: String = "",
    override val getCause: Throwable = null
) extends ActionException(getMessage_simple, getCause)
    with HasCoreDump

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
//class QueryException(
//    override val getMessage_simple: String = "",
//    override val getCause: Throwable = null
//) extends SpookyException

//class BrowserDeploymentException(
//    override val getMessage_simple: String = "",
//    override val getCause: Throwable = null
//) extends SpookyException
