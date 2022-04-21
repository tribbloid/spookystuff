package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.{CommonUtils, TimeoutConf}
import com.tribbloids.spookystuff.{ActionException, Const, SpookyContext}
import org.apache.spark.ml.dsl.utils.refl.ScalaUDT
import org.apache.spark.sql.types.SQLUserDefinedType
import org.openqa.selenium.WebElement
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import org.slf4j.LoggerFactory

import java.util

class ActionUDT extends ScalaUDT[Action]

/**
  * These are the same actions a human would do to get to the data page,
  * their order of execution is identical to that they are defined.
  * Many supports **Cell Interpolation**: you can embed cell reference in their constructor
  * by inserting keys enclosed by `'{}`, in execution they will be replaced with values they map to.
  * This is used almost exclusively in typing into an url bar or textbox, but it's flexible enough to be used anywhere.
  * extends Product to make sure all subclasses are case classes
  */
//TODO: merging with Extractor[Seq[Fetched]]?
@SQLUserDefinedType(udt = classOf[ActionUDT])
trait Action extends ActionLike {

  override def children: Trace = Nil

  var timeElapsed: Long = -1 //only set once

  override def dryrun: List[List[Action]] = {
    if (hasOutput) {
      List(List(this))
    } else {
      List()
    }
  }

  //this should handle autoSave, cache and errorDump
  final override def apply(session: Session): Seq[DocOption] = {

    val results = try {
      exe(session)
    } catch {
      case e: Exception =>
        val message: String = getSessionExceptionMessage(session)

        val ex = e match {
          case ae: ActionException => ae
          case _                   => new ActionException(message, e)
        }
//        ex.setStackTrace(e.getStackTrace)
        throw ex
    }

    this.timeElapsed = System.currentTimeMillis() - session.startTime
    session.spooky.spookyMetrics.pagesFetchedFromRemote += results.count(_.isInstanceOf[Doc])

    results
  }

  //execute errorDumps as side effects
  protected def getSessionExceptionMessage(
      session: Session,
      docOpt: Option[Doc] = None
  ): String = {
    var message: String = "\n{\n"

    message += {
      session.backtrace.map { action =>
        "| " + action.toString
      } ++
        Seq("+> " + this.detailedStr)
    }.mkString("\n")

    val errorDump: Boolean = session.spooky.spookyConf.errorDump
    val errorDumpScreenshot: Boolean = session.spooky.spookyConf.errorScreenshot

    message += "\n}"

    session match {
      case d: Session =>
        if (d.webDriverOpt.nonEmpty) {
          if (errorDump) {
            val rawPage = ErrorDump.exe(session).head.asInstanceOf[Doc]
            message += "\nSnapshot: " + this.errorDump(message, rawPage, session.spooky)
          }
          if (errorDumpScreenshot) {
            try {
              val rawPage = ErrorScreenshot.exe(session).head.asInstanceOf[Doc]
              message += "\nScreenshot: " + this.errorDump(message, rawPage, session.spooky)
            } catch {
              case e: Exception =>
                LoggerFactory.getLogger(this.getClass).error("Cannot take screenshot on ActionError:", e)
            }
          }
        } else {
          docOpt.foreach { doc =>
            if (errorDump) {
              message += "\nSnapshot: " + this.errorDump(message, doc, session.spooky)
            }
          }
        }
    }
    message
  }

  protected def errorDump(message: String, rawPage: Doc, spooky: SpookyContext): String = {

    val backtrace =
      if (rawPage.uid.backtrace.lastOption.exists(_ eq this)) rawPage.uid.backtrace
      else rawPage.uid.backtrace :+ this
    val uid = rawPage.uid.copy(backtrace = backtrace)(name = null)
    val page = rawPage.copy(uid = uid)
    try {
      page.errorDump(spooky)
      "saved to: " + page.saved.last
    } catch {
      case e: Exception =>
        try {
          page.errorDumpLocally(spooky)
          "DFS inaccessible.........saved to: " + page.saved.last
        } catch {
          case e: Exception =>
            "all file systems inaccessible.........not saved"
        }
    }
  }

  protected[actions] def withDriversDuring[T](session: Session)(f: => T): T = {

    var baseStr = s"[${session.taskContextOpt.map(_.partitionId()).getOrElse(0)}]+> ${this.toString}"
    this match {
      case timed: Timed =>
        baseStr = baseStr + s" in ${timed.timeout(session)}"
        LoggerFactory.getLogger(this.getClass).info(this.withDetail(baseStr))

        session.withDriversDuring {
          session.progress.ping()

          CommonUtils.withTimeout(timed.hardTerminateTimeout(session))(
            f,
            session.progress.defaultHeartbeat
          )
        }
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(this.withDetail(baseStr))

        session.withDriversDuring(
          f
        )
    }
  }

  final protected[actions] def exe(session: Session): Seq[DocOption] = {
    withDriversDuring(session) {
      doExe(session)
    }
  }

  protected def doExe(session: Session): Seq[DocOption]

  def andThen(f: Seq[DocOption] => Seq[DocOption]): Action = AndThen(this, f)

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.timeElapsed = same.asInstanceOf[Action].timeElapsed
  }
}

trait Timed extends Action {

  var _timeout: TimeoutConf = _

  def in(timeout: TimeoutConf): this.type = {
    this._timeout = timeout
    this
  }

  def timeout(session: Session): TimeoutConf = {
    val base =
      if (this._timeout == null) session.spooky.spookyConf.remoteResourceTimeout
      else this._timeout

    base
  }

  def hardTerminateTimeout(session: Session): TimeoutConf = {
    val original = timeout(session)
    original.copy(max = original.max + Const.hardTerminateOverhead)
  }

  def webDriverWait(session: Session): WebDriverWait =
    new WebDriverWait(session.webDriver, this.timeout(session).max.toSeconds)

  def getClickableElement(selector: Selector, session: Session): WebElement = {

    val elements = webDriverWait(session).until(ExpectedConditions.elementToBeClickable(selector.by))

    elements
  }

  def getElement(selector: Selector, session: Session): WebElement = {

    val elements = webDriverWait(session).until(ExpectedConditions.presenceOfElementLocated(selector.by))

    elements
  }

  def getElements(selector: Selector, session: Session): util.List[WebElement] = {

    val elements = webDriverWait(session).until(ExpectedConditions.presenceOfAllElementsLocatedBy(selector.by))

    elements
  }

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this._timeout = same.asInstanceOf[Timed]._timeout
  }
}

trait Named extends Action {

  var nameOpt: Option[String] = None
  def name: String = nameOpt.getOrElse(this.toString)

  def as(name: Symbol): this.type = {
    assert(name != null)

    this.nameOpt = Some(name.name)
    this
  }

  final def ~(name: Symbol): this.type = as(name)

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.nameOpt = same.asInstanceOf[Named].nameOpt
  }
}

trait Driverless extends Action {}

trait ActionPlaceholder extends Action {

  override protected def doExe(session: Session): Seq[DocOption] = {
    throw new UnsupportedOperationException(s"${this.getClass.getSimpleName} is a placeholder")
  }
}
