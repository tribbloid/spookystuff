package com.tribbloids.spookystuff.session

import java.util.regex.{Matcher, Pattern}

import com.tribbloids.spookystuff.PythonException

import scala.util.{Failure, Success, Try}

/**
  * Created by peng on 01/08/16.
  */
//case class PythonException(
//
//                          ) extends SpookyException()

case class PythonDriver(
                         binPath: String
                       ) extends PythonProcess(binPath) with Clean {

  //  def
  override def clean(): Unit = {
    this.close()
  }

  private val errorInLastLine: Pattern = Pattern.compile(".*(Error|Exception): .*$")

  /**
    * Checks if there is a syntax error or an exception
    * From Zeppelin PythonInterpreter
    *
    * @param output Python interpreter output
    * @return true if syntax error or exception has happened
    */
  private def pythonErrorIn(output: String): Boolean = {
    val errorMatcher: Matcher = errorInLastLine.matcher(output)
    errorMatcher.find
  }

  def interpret(code: String): Array[String] = {
    val output = this.sendAndGetResult(code)
    val rows: Array[String] = output.split("\n")
      .map(
        _.stripPrefix(">>>").trim
      )

    if (pythonErrorIn(output)) {
      val ee = new PythonException(
        rows.mkString("\n")
      )
      throw ee
    }
    else {
      rows
    }
  }
}
