package com.tribbloids.spookystuff.session

import java.util.regex.Pattern

import com.tribbloids.spookystuff.PythonException
import com.tribbloids.spookystuff.utils.SpookyUtils

object PythonDriver {

  final val DEFAULT_PYTHON_PATH = System.getProperty("user.dir") + "/temp"
  final val MODULE_NAME = "pyspookystuff"
  final val RESOURCE_NAME = "com/tribbloids/" + MODULE_NAME

  final val errorInLastLine: Pattern = Pattern.compile(".*(Error|Exception):.*$")
}

/**
  * Created by peng on 01/08/16.
  */
//TODO: not reusing Python worker for spark, is it not optimal?
case class PythonDriver(
                         executable: String,
                         pythonPath: String = PythonDriver.DEFAULT_PYTHON_PATH // extract pyspookystuff from resources temporarily on workers
                       ) extends PythonProcess(executable) with CleanMixin {

  import com.tribbloids.spookystuff.utils.ImplicitUtils._

  @transient final val modulePath = pythonPath :/ PythonDriver.MODULE_NAME

  {
    val resourceOpt = SpookyUtils.getCPResource(PythonDriver.RESOURCE_NAME)
    resourceOpt.foreach {
      resource =>
        SpookyUtils.asynchIfNotExist(modulePath){

          SpookyUtils.extractResource(resource, modulePath)
        }
    }

    this.open()

    // TODO: add setup modules using pip

    this.interpret(
      s"""
         |import sys
         |sys.path.append('$pythonPath')
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
    * @return true if syntax error or exception has happened
    */
  private def pythonErrorIn(lines: Seq[String]): Boolean = {

    val indexed = lines.zipWithIndex
    val tracebackRows: Seq[Int] = indexed.filter(_._1.startsWith("Traceback ")).map(_._2)
    val errorRows: Seq[Int] = indexed.filter{
      v =>
        val matcher = PythonDriver.errorInLastLine.matcher(v._1)
        matcher.find
    }.map(_._2)

    if (tracebackRows.nonEmpty && errorRows.nonEmpty) true
    else false

//    tracebackRows.foreach {
//      row =>
//        val errorRowOpt = errorRows.find(_ > row)
//        errorRowOpt.foreach {
//          errorRow =>
//            val tracebackDetails = lines.slice(row +1, errorRow)
//            if (tracebackDetails.forall(_.startsWith(""))
//        }
//    }
  }

  final def PROMPTS = "^(>>>|\\.\\.\\.| )+"

  def removePrompts(str: String): String = {
    str.trim.replaceAll(PROMPTS, "")
  }

  def interpret(code: String): Array[String] = {
    val output = this.sendAndGetResult(code)
    val rows: Array[String] = output
      .split("\n")
      .map(
        removePrompts
      )

    if (pythonErrorIn(rows)) {
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
