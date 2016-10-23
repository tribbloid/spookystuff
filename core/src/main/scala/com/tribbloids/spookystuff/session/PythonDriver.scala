package com.tribbloids.spookystuff.session

import java.util.regex.Pattern

import com.tribbloids.spookystuff.PythonException
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.slf4j.LoggerFactory

object PythonDriver {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  final val DEFAULT_PYTHON_PATH = System.getProperty("user.home") \\ ".spookystuff" \\ "pythonpath"
  final val MODULE_NAME = "pyspookystuff"
  final val MODULE_RESOURCE = "com/tribbloids/" :/ MODULE_NAME
  final val PYTHON_LIB_RESOURCE = "com/tribbloids/spookystuff/lib/python"

  final val errorInLastLine: Pattern = Pattern.compile(".*(Error|Exception):.*$")

  import com.tribbloids.spookystuff.utils.SpookyViews._

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

  def NO_RETURN_VALUE: String = {
    "*!?no returned value!?*"
  }

  def EXECUTION_RESULT: String = {
    "*!?execution result!?*"
  }
}

/**
  * Created by peng on 01/08/16.
  */
//TODO: not reusing Python worker for spark, is it not optimal?
case class PythonDriver(
                         executable: String,
                         autoImports: String =
                         """
                           |import sys
                           |import os
                           |import json
                         """.trim.stripMargin,
                         override val taskOrThread: TaskOrThread
                       ) extends PythonProcess(executable) with AutoCleanable {

  {
    val pythonPath = PythonDriver.deploy

    this.open

    this.interpret(
      s"""
         |$autoImports
         |sys.path.append('$pythonPath')
       """.stripMargin
    )
  }

  import PythonDriver._

  //avoid reopening!
  override lazy val open = super.open()

  override def _clean(): Unit = {
    try {
      this.close()
    }
    catch {
      case e: Throwable =>
        this.interrupt()
    }
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
        val matcher = errorInLastLine.matcher(v._1)
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

  final def PROMPTS = "^(>>> |\\.\\.\\. )+"

  def removePrompts(str: String): String = {
    str.stripPrefix("\r").replaceAll(PROMPTS, "")
  }

  def interpret(code: String): Array[String] = {
    val indentedCode = code.split('\n').filter(_.nonEmpty).map(">>> " + _).mkString("\n")

    LoggerFactory.getLogger(this.getClass).debug("============== PYTHON INPUT ===============\n" + indentedCode)

    val output = this.sendAndGetResult(code)
    val rows: Array[String] = output
      .split("\n")
      .map(
        removePrompts
      )

    if (rows.exists(_.nonEmpty)) {
      LoggerFactory.getLogger(this.getClass).info("============== PYTHON OUTPUT ===============\n" + rows.mkString("\n"))
    }

    if (pythonErrorIn(rows)) {
      val ee = new PythonException(
        indentedCode,
        rows.mkString("\n")
      )
      throw ee
    }

    rows
  }

  def execute(code: String, resultVar: String = "result"): (Seq[String], Option[String]) = {
    val _code =
      s"""
         |$resultVar = None
         |
        |$code
         |
        |print('$EXECUTION_RESULT')
         |if $resultVar: print($resultVar)
         |else: print('$NO_RETURN_VALUE')
      """.stripMargin

    val rows = interpret(_code).toSeq
    val splitterI = rows.zipWithIndex.find(_._1 == EXECUTION_RESULT).get._2
    val splitted = rows.splitAt(splitterI)

    val _result = splitted._2.slice(1, Int.MaxValue).mkString("\n")
    val resultOpt = if (_result == NO_RETURN_VALUE) None
    else Some(_result)

    splitted._1 -> resultOpt
  }
}
