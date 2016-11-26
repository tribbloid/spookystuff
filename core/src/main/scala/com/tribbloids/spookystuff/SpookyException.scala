package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.utils.TreeException

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

class SpookyException(
                       val message: String = "",
                       override val cause: Throwable = null
                     ) extends TreeException.Unary(message, cause) {

}

class ActionException(
                       override val message: String = "",
                       override val cause: Throwable = null
                     ) extends SpookyException(message, cause) {

}

class PyException(
                   val code: String,
                   val output: String,
                   override val cause: Throwable = null,
                   val historyCodeOpt: Option[String] = None
                 ) extends SpookyException(
  {
    s"""
       |${historyCodeOpt.map(v => "\t### History ###\n" + v).getOrElse("")}
       |${"\t"}# Error interpreting:
       |$code
       |---------------------------------------
       |$output
     """.stripMargin
  },
  cause
)

class PyInterpreterException(
                              code: String,
                              output: String,
                              historyCodeOpt: Option[String] = None
                            ) extends PyException(code, output, null, historyCodeOpt)

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
