package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.{Doc, Fetched}
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.selenium.BySizzleCssSelector
import com.tribbloids.spookystuff.session.{DriverSession, NoDriverSession, Session}
import com.tribbloids.spookystuff.utils.{AnyUDT, Utils}
import com.tribbloids.spookystuff.{ActionException, Const, SpookyContext}
import org.apache.spark.sql.types.SQLUserDefinedType
import org.openqa.selenium.TakesScreenshot
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

class ActionUDT extends AnyUDT[Action]

/**
  * These are the same actions a human would do to get to the data page,
  * their order of execution is identical to that they are defined.
  * Many supports **Cell Interpolation**: you can embed cell reference in their constructor
  * by inserting keys enclosed by `'{}`, in execution they will be replaced with values they map to.
  * This is used almost exclusively in typing into an url bar or textbox, but it's flexible enough to be used anywhere.
  * extends Product to make sure all subclasses are case classes
  */
//TODO: merging with Selector[Seq[Fetched]]?
@SQLUserDefinedType(udt = classOf[ActionUDT])
trait Action extends ActionLike {

  override def children: Seq[Action] = Seq()

  private var timeElapsed: Long = -1 //only set once

  override def dryrun = {
    if (hasOutput){
      List(List(this))
    }
    else {
      List()
    }
  }

  //this should handle autoSave, cache and errorDump
  def apply(session: Session): Seq[Fetched] = {

    val results = try {
      exe(session)
    }
    catch {
      case e: Throwable =>

        val message: String = getSessionExceptionString(session)

        val ex = e match {
          case ae: ActionException => ae
          case _ =>new ActionException(message, e)
        }
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }

    this.timeElapsed = System.currentTimeMillis() - session.startTime

    results
  }

  //execute errorDumps as side effects
  protected def getSessionExceptionString(
                                           session: Session,
                                           docOpt: Option[Doc] = None
                                         ): String = {
    var message: String = "\n{"

    message += session.backtrace.map {
      action =>
        "| " + action.toString
    }.mkString("\n")

    message += "\n+> " + this.toString

    val errorDump: Boolean = session.spooky.conf.errorDump
    val errorDumpScreenshot: Boolean = session.spooky.conf.errorScreenshot

    message += "}"

    session match {
      case d: DriverSession =>
        if (errorDump) {
          val rawPage = DefaultSnapshot.exe(session).head.asInstanceOf[Doc]
          message += "\nSnapshot: " + this.errorDump(message, rawPage, session.spooky)
        }
        if (errorDumpScreenshot && session.driver.isInstanceOf[TakesScreenshot]) {
          val rawPage = DefaultScreenshot.exe(session).toList.head.asInstanceOf[Doc]
          message += "\nScreenshot: " + this.errorDump(message, rawPage, session.spooky)
        }
      case d: NoDriverSession =>
        docOpt.foreach{
          doc =>
            if (errorDump) {
              message += "\nSnapshot: " + this.errorDump(message, doc, session.spooky)
            }
        }
    }
    message
  }

  protected def errorDump(message: String, rawPage: Doc, spooky: SpookyContext): String = {

    val backtrace = if (rawPage.uid.backtrace.lastOption.exists(_ eq this)) rawPage.uid.backtrace
    else rawPage.uid.backtrace :+ this
    val uid = rawPage.uid.copy(backtrace = backtrace)
    val page = rawPage.copy(uid = uid)
    try {
      page.errorDump(spooky)
      "saved to: " + page.saved.last
    }
    catch {
      case e: Throwable =>
        try {
          page.errorDumpLocally(spooky)
          "DFS inaccessible.........saved to: " + page.saved.last
        }
        catch {
          case e: Throwable =>
            "all file systems inaccessible.........not saved"
        }
    }
  }

  protected[actions] def exe(session: Session): Seq[Fetched] = {

    this match { //temporarily disabled as we assume that DFS is the culprit for causing deadlock
      case tt: Timed =>
        LoggerFactory.getLogger(this.getClass).info(s"+> ${this.toString} in ${tt.timeout(session)}")

        Utils.withDeadline(tt.hardTerminateTimeout(session)) {
          doExe(session)
        }
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(s"+> ${this.toString}")

        doExe(session)
    }
  }

  protected def doExe(session: Session): Seq[Fetched]

  def andThen(f: Seq[Fetched] => Seq[Fetched]): Action = AndThen(this, f)

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.timeElapsed = same.asInstanceOf[Action].timeElapsed
  }
}

trait Timed extends Action {

  private var _timeout: Duration = null

  def in(deadline: Duration): this.type = {
    this._timeout = deadline
    this
  }

  def timeout(session: Session): Duration = {
    val base = if (this._timeout == null) session.spooky.conf.remoteResourceTimeout
    else this._timeout

    base
  }

  def hardTerminateTimeout(session: Session): Duration = {
    timeout(session) + Const.hardTerminateOverhead
  }

  def driverWait(session: Session) = new WebDriverWait(session.driver, this.timeout(session).toSeconds)

  def getClickableElement(selector: String, session: Session) = {

    val elements = driverWait(session).until(ExpectedConditions.elementToBeClickable(new BySizzleCssSelector(selector)))

    elements
  }

  def getElement(selector: String, session: Session) = {

    val elements = driverWait(session).until(ExpectedConditions.presenceOfElementLocated(new BySizzleCssSelector(selector)))

    elements
  }

  def getElements(selector: String, session: Session) = {

    val elements = driverWait(session).until(ExpectedConditions.presenceOfAllElementsLocatedBy(new BySizzleCssSelector(selector)))

    elements
  }

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this._timeout = same.asInstanceOf[Timed]._timeout
  }
}

trait Named extends Action {

  var name: String = this.toString

  def as(name: Symbol): this.type = {
    assert(name != null)

    this.name = name.name
    this
  }

  final def ~(name: Symbol): this.type = as(name)

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.name = same.asInstanceOf[Named].name
  }
}

trait Driverless extends Action {

  override def needDriver = false
}

trait Wayback extends Action {

  def wayback: Extractor[Long]
}