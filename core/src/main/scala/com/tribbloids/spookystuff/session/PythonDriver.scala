package com.tribbloids.spookystuff.session

import java.io.File
import java.util.regex.{Matcher, Pattern}

import com.tribbloids.spookystuff.PythonException
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.commons.io.FileUtils

object PythonDriver {

  final val DEFAULT_TEMP_PATH = System.getProperty("user.dir") + "/temp/pythonpath/pyspookystuff"
  final val RESOURCE_PATH = "com/tribbloids/pyspookystuff"

  final val errorInLastLine: Pattern = Pattern.compile(".*(Error|Exception): .*$")
}

/**
  * Created by peng on 01/08/16.
  */
//TODO: not reusing Python worker for spark, is it not optimal?
case class PythonDriver(
                         binPath: String,
                         tempPath: String = PythonDriver.DEFAULT_TEMP_PATH // extract pyspookystuff from resources temporarily on workers
                       ) extends PythonProcess(binPath) with CleanMixin {
  {
    val resource = SpookyUtils.getCPResource(PythonDriver.RESOURCE_PATH).get.toURI
    val src = new File(resource)

    FileUtils.copyDirectory(src, new File(tempPath))

    this.open()

    // TODO: add setup modules from pyPI

    this.interpret(
      s"""
         |import sys
         |sys.path.append('$tempPath')
       """.stripMargin
    )
  }

  override def clean(): Unit = {
    this.close()
  }

  /**
    * Checks if there is a syntax error or an exception
    * From Zeppelin PythonInterpreter
    *
    * @param output Python interpreter output
    * @return true if syntax error or exception has happened
    */
  private def pythonErrorIn(output: String): Boolean = {
    val errorMatcher: Matcher = PythonDriver.errorInLastLine.matcher(output)
    errorMatcher.find
  }

  final def PROMPT = "^(>>>|\\.\\.\\.| )+"

  def removePrompts(str: String): String = {
    str.trim.replaceAll(PROMPT, "")
  }

  def interpret(code: String): Array[String] = {
    val output = this.sendAndGetResult(code)
    val rows: Array[String] = output
      .split("\n")
      .map(
        removePrompts
      )

    if (pythonErrorIn(output)) {
      val ee = new PythonException(
        "Error interpreting" +
          "\n>>>\n" +
          code +
          "\n---\n" +
          rows.mkString("\n")
      )
      throw ee
    }
    else {
      rows
    }
  }
}
