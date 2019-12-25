package com.tribbloids.spookystuff

import java.io.IOException

import com.tribbloids.spookystuff.utils.TreeThrowable

/**
  * Created by peng on 9/11/14.
  * doesn't have to catch it every time
  */
////TODO: merge with MultiCauses
//class TreeCauses(
//                  message: String = "",
//                  cause: Throwable = null
//                ) extends RuntimeException(message, cause) {
//
//  override def getMessage: String = {
//    //    if (cause == null) {
//    messageStr
//    //    }
//    //    else {
//    //      s"$messageStr\nCaused by: ${this.getCause}"
//    //    }
//  }
//
//  def messageStr: String = {
//    this.message
//  }
//}

trait SpookyException extends TreeThrowable {

  def cause: Throwable = null

  override def getCause: Throwable = cause
}

class ActionException(
    override val simpleMsg: String = "",
    override val cause: Throwable = null
) extends SpookyException {}

class PyException(
    val code: String,
    val output: String,
    override val cause: Throwable = null,
    val historyCodeOpt: Option[String] = None
) extends SpookyException {

  override def simpleMsg: String =
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
    override val simpleMsg: String = "",
    override val cause: Throwable = null
) extends ActionException(simpleMsg, cause)

class DFSReadException(
    override val simpleMsg: String = "",
    override val cause: Throwable = null
) extends IOException(simpleMsg, cause)
    with SpookyException

class DFSWriteException(
    override val simpleMsg: String = "",
    override val cause: Throwable = null
) extends IOException(simpleMsg, cause)
    with SpookyException

//TODO: cause confusion! replace with IllegalArgumentException or use mixin
class QueryException(
    override val simpleMsg: String = "",
    override val cause: Throwable = null
) extends SpookyException

class BrowserDeploymentException(
    override val simpleMsg: String = "",
    override val cause: Throwable = null
) extends SpookyException
