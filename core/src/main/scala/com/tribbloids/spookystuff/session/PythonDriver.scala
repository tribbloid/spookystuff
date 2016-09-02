package com.tribbloids.spookystuff.session

import java.util.regex.{Matcher, Pattern}

import com.tribbloids.spookystuff.PythonException

/**
  * Created by peng on 01/08/16.
  */
//case class PythonException(
//
//                          ) extends SpookyException()
//TODO: not reusing Python worker for spark, is it not optimal?
case class PythonDriver(
                         binPath: String
                       ) extends PythonProcess(binPath) with Cleanable {

  //open lazily
  @transient lazy val opened: Unit = this.open()

  override def clean(): Unit = {
    try {
      this.close()
    }
    catch {
      case e: NullPointerException =>
    }
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

  final def PROMPT = ">>> "

  def removeLeading_>>>(str: String): String = {
    val trimmed = str.trim
    if (trimmed.startsWith(PROMPT)) {
      val removed = trimmed.stripPrefix(PROMPT)
      removeLeading_>>>(removed)
    }
    else {
      trimmed
    }
  }

  def interpret(code: String): Array[String] = {
    this.opened
    val output = this.sendAndGetResult(code)
    val rows: Array[String] = output
      .split("\n")
      .map(
        removeLeading_>>>
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
