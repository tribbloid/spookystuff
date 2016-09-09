package com.tribbloids.spookystuff.session

import java.util.regex.Pattern

import com.tribbloids.spookystuff.PythonException
import com.tribbloids.spookystuff.utils.SpookyUtils

object PythonDriver {

  final val DEFAULT_PYTHON_PATH = System.getProperty("user.dir") + "/temp/python"
  final val MODULE_NAME = "pyspookystuff"
  final val MODULE_RESOURCE = "com/tribbloids/" + MODULE_NAME
  final val PYTHON_LIB_RESOURCE = "com/tribbloids/spookystuff/lib/python"

  final val errorInLastLine: Pattern = Pattern.compile(".*(Error|Exception):.*$")

  import com.tribbloids.spookystuff.utils.ImplicitUtils._

  lazy val deploy: String = {
    val pythonPath: String = PythonDriver.DEFAULT_PYTHON_PATH // extract pyspookystuff from resources temporarily on workers
    val modulePath = pythonPath :/ PythonDriver.MODULE_NAME

    val libResourceOpt = SpookyUtils.getCPResource(PythonDriver.PYTHON_LIB_RESOURCE)
    libResourceOpt.foreach {
      resource =>
        //        SpookyUtils.asynchIfNotExist(pythonPath){

        SpookyUtils.extractResource(resource, pythonPath)
      //        }
    }

    val moduleResourceOpt = SpookyUtils.getCPResource(PythonDriver.MODULE_RESOURCE)
    moduleResourceOpt.foreach {
      resource =>
        //        SpookyUtils.asynchIfNotExist(modulePath){

        SpookyUtils.extractResource(resource, modulePath)
      //        }
    }
    pythonPath
  }
}

/**
  * Created by peng on 01/08/16.
  */
//TODO: not reusing Python worker for spark, is it not optimal?
case class PythonDriver(
                         executable: String
                       ) extends PythonProcess(executable) with CleanMixin {

  {
    val pythonPath = PythonDriver.deploy

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
