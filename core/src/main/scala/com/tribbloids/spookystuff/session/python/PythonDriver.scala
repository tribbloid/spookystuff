package com.tribbloids.spookystuff.session.python

import java.io.File
import java.util.regex.Pattern

import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CommonUtils, SilentRetry, SpookyUtils}
import com.tribbloids.spookystuff.{PyException, PyInterpretationException, SpookyContext}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.dsl.utils.FlowUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.TimeoutException
import scala.util.Try

object PythonDriver {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  final val DEFAULT_PYTHON_PATH = System.getProperty("user.home") \\ ".spookystuff" \\ "pythonpath"
  final val MODULE_NAME = "pyspookystuff"
  final val MODULE_RESOURCE = "com/tribbloids/" :/ MODULE_NAME
  final val PYTHON_LIB_RESOURCE = "com/tribbloids/spookystuff/lib/python"

  final val errorPattern: Pattern = Pattern.compile(".*(Error|Exception):.*$")
  final val syntaxErrorPattern: Pattern = Pattern.compile(".*(SyntaxError):.*$")

  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * DO NOT ATTEMPT TO SIMPLIFY IMPLEMENTATION!
    * com.tribbloids.pyspookystuff exists in both /class & /test-class and any attempt to merge it
    * with com.tribbloids.spookystuff.lib.python will results in test classes being retrieved.
    */
  lazy val deploy: String = {
    val pythonPath: String = PythonDriver.DEFAULT_PYTHON_PATH // extract pyspookystuff from resources temporarily on workers
    val modulePath = pythonPath \\ PythonDriver.MODULE_NAME

    val pythonDir = new File(pythonPath)
    FileUtils.deleteQuietly(pythonDir)

    val libResourceOpt = SpookyUtils.getCPResource(PythonDriver.PYTHON_LIB_RESOURCE)
    libResourceOpt.foreach {
      resource =>
        SpookyUtils.extractResource(resource, pythonPath)
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

  val NO_RETURN_VALUE: String =   "======== *!?no return value!?* ========"
  val EXECUTION_RESULT: String =  "======== *!?execution result!?* ========"
  val ERROR_INFO: String =        "======== *!?error info!?* ========"

  /**
    * Checks if there is a syntax error or an exception
    * From Zeppelin PythonInterpreter
    * HIGHLY VOLATILE: doesn't always work
    */
  def primitiveErrorIn(lines: Seq[String]): Boolean = {

    val indexed = lines.zipWithIndex
    val tracebackRows: Seq[Int] = indexed.filter(_._1.startsWith("Traceback ")).map(_._2)
    val errorRows: Seq[Int] = indexed.filter {
      v =>
        val matcher = errorPattern.matcher(v._1)
        matcher.find
    }.map(_._2)

    if ((tracebackRows.nonEmpty && errorRows.nonEmpty) || syntaxErrorIn(lines)) true
    else false
  }

  def syntaxErrorIn(lines: Seq[String]): Boolean = {
    val syntaxErrorLine = lines.filter {
      v =>
        val matcher = syntaxErrorPattern.matcher(v)
        matcher.find
    }
    syntaxErrorLine.nonEmpty
  }
}

/**
  * Created by peng on 01/08/16.
  */
//TODO: not reusing Python worker for spark, is it not optimal?
class PythonDriver(
                    val executable: String = "python",
                    val autoImports: String =
                    """
                      |import os
                      |from __future__ import print_function
                    """.trim.stripMargin,
                    override val _lifespan: Lifespan = new Lifespan.Auto()
                  ) extends PythonProcess(executable) with LocalCleanable {

  import scala.concurrent.duration._

  /**
    * NOT thread safe
    */
  lazy val historyLines: ArrayBuffer[String] = ArrayBuffer.empty
  lazy val pendingLines: ArrayBuffer[String] = ArrayBuffer.empty
  lazy val importedLines: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty

  import PythonDriver._

  def historyCodeOpt = {
    if (this.historyLines.isEmpty) None
    else {
      val combined = "\n" + this.historyLines.mkString("\n").stripPrefix("\n")
      val indentedCode = FlowUtils.indent(combined)

      Some(indentedCode)
    }
  }

  {
    val pythonPath = PythonDriver.deploy

    this.open

    this.batchImport(
      s"""
         |import sys
         |sys.path.append('$pythonPath')
         |$autoImports
       """.stripMargin
        .split("\n")
    )
  }

  //avoid reopening!
  override lazy val open: Unit = {
    super.open()
  }

  override def cleanImpl(): Unit = {
    Try {
      CommonUtils.retry(5) {
        try { if (process.isAlive) {
          CommonUtils.withDeadline(3.seconds) {
            try {
              this._interpret("exit()")
            }
            catch {
              case e: PyException =>
            }
          }
          Thread.sleep(1000)
          assert(!process.isAlive)
        }}
        catch {
          case e: TimeoutException =>
            throw SilentRetry(e)
          case e: Throwable =>
            throw e
        }
      }
    }
      .orElse(
        Try(this.close())
      )
      .getOrElse(this.interrupt())
  }

  final def PROMPTS = "^(>>> |\\.\\.\\. )+"

  def removePrompts(str: String): String = {
    str.stripPrefix("\r").replaceAll(PROMPTS, "")
  }

  override def logPyOutput(line: String) = {
    val effectiveLine = removePrompts(line)
    s"$logPrefix $effectiveLine"
  }

  private def _interpret(code: String, spookyOpt: Option[SpookyContext] = None, detectError: Boolean = true): Array[String] = {
    val indentedCode = FlowUtils.indent(code)

    LoggerFactory.getLogger(this.getClass).debug(s">>> $logPrefix INPUT >>>\n" + indentedCode)

    val rows = try {
      // DO NOT DELETE! some Python Drivers are accessed by many threads (e.g. ProxyManager)
      val output = this.synchronized {
        this.sendAndGetResult(code)
      }
      output
        .split("\n")
        .map(
          removePrompts
        )
    }
    catch {
      case e: Throwable =>
        spookyOpt.foreach(
          _.spookyMetrics.pythonInterpretationError += 1
        )
        val cause = e
        if (this.isCleaned) {
          LoggerFactory.getLogger(this.getClass).debug(
            s"ignoring ${cause.getClass.getSimpleName}, python process is cleaned"
          )
          return Array.empty[String]
        }
        else {
          val ee = new PyException(
            indentedCode,
            this.outputBuffer,
            cause,
            historyCodeOpt
          )
          throw ee
        }
    }

    //    if (rows.exists(_.nonEmpty)) {
    //      LoggerFactory.getLogger(this.getClass).info(s"$$$$$$ PYTHON-${this.taskOrThread.id} OUTPUT ===============\n" + rows.mkString("\n"))
    //    }
    //    else {
    //      LoggerFactory.getLogger(this.getClass).info(s"$$$$$$ PYTHON-${this.taskOrThread.id} [NO OUTPUT] ===============\n" + rows.mkString("\n"))
    //    }

    val hasError = if (detectError) primitiveErrorIn(rows)
    else syntaxErrorIn(rows)

    if (hasError) {
      spookyOpt.foreach(
        _.spookyMetrics.pythonInterpretationError += 1
      )
      val ee = PyInterpretationException(
        indentedCode,
        rows.mkString("\n"),
        historyCodeOpt = historyCodeOpt
      )
      throw ee
    }

    rows
  }

  private def _interpretCaptureError(code: String, spookyOpt: Option[SpookyContext] = None): Array[String] = {
    val indentedCode = FlowUtils.indent(code)
    val codeTryExcept =
      s"""
         |try:
         |$indentedCode
         |except Exception as e:
         |    print('$ERROR_INFO')
         |    raise
       """.stripMargin

    val rows = _interpret(codeTryExcept, detectError = false)

    val splitterIndexOpt = rows.zipWithIndex.find(_._1 == ERROR_INFO)
    splitterIndexOpt match {
      case None =>
      case Some(i) =>
        val split = rows.splitAt(i._2)
        throw PyInterpretationException(
          indentedCode,
          split._2.slice(1, Int.MaxValue).mkString("\n"),
          historyCodeOpt = historyCodeOpt
        )
    }

    spookyOpt.foreach(
      _.spookyMetrics.pythonInterpretationSuccess += 1
    )

    rows
  }

  //TODO: due to unchecked use of thread-unsafe mutable objects (e.g. ArrayBuffer), all following APIs are rendered synchronized.
  def interpret(
                 code: String,
                 spookyOpt: Option[SpookyContext] = None
               ): Array[String] = this.synchronized{
    def lazyCode = pendingLines.mkString("\n")
    val allCode = lazyCode + "\n" + code
    val result = _interpretCaptureError(allCode, spookyOpt)
    this.historyLines += allCode
    this.pendingLines.clear()
    result
  }

  /**
    *
    * @return stdout strings -> print(resultVar)
    */
  def eval(
            code: String,
            resultVarOpt: Option[String] = None,
            spookyOpt: Option[SpookyContext] = None
          ): (Seq[String], Option[String]) = this.synchronized{
    resultVarOpt match {
      case None =>
        val _code =
          s"""
             |$code
          """.trim.stripMargin
        val rows = interpret(_code, spookyOpt)
        rows.toSeq -> None
      case Some(resultVar) =>
        val _code =
          s"""
             |$resultVar=None
             |$code
             |print('$EXECUTION_RESULT')
             |if $resultVar:
             |    print($resultVar)
             |else:
             |    print('$NO_RETURN_VALUE')
             |
             |del($resultVar)
          """.trim.stripMargin
        val rows = interpret(_code, spookyOpt).toSeq
        val splitterIndex = rows.zipWithIndex.find(_._1 == EXECUTION_RESULT)
          .getOrElse {
            assertNotCleaned("Empty output")
            if (!this.process.isAlive)
              throw new AssertionError(s"$logPrefix python driver is dead")
            else
              throw new AssertionError(s"$logPrefix Cannot find $EXECUTION_RESULT\n" + rows.mkString("\n"))
          }._2
        val split = rows.splitAt(splitterIndex)

        val _result = split._2.slice(1, Int.MaxValue).mkString("\n")
        val resultOpt = if (_result == NO_RETURN_VALUE) None
        else Some(_result)

        split._1 -> resultOpt
    }
  }


  def evalExpr(expr: String, spookyOpt: Option[SpookyContext] = None): Option[String] = {
    val tempName = "_temp" + SpookyUtils.randomSuffix
    val result = eval(
      s"""
         |$tempName=$expr
            """.trim.stripMargin,
      Some(tempName),
      spookyOpt
    )
    result._2
  }

  def lazyInterpret(code: String): Unit = this.synchronized{
    pendingLines += code
  }

  def batchImport(codes: Seq[String]): Unit = this.synchronized{
    val effectiveCodes = ArrayBuffer[String]()
    codes
      .map(_.trim)
      .foreach {
        code =>
          if (!importedLines.contains(code)) {
            effectiveCodes += code
            importedLines += code
          }
      }
    this._interpret(
      effectiveCodes.mkString("\n"),
      None,
      detectError = true
    )
  }
}
