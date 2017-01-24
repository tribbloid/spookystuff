package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.{Doc, Fetched}
import com.tribbloids.spookystuff.selenium.BySizzleCssSelector
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.{ScalaUDT, SpookyUtils}
import com.tribbloids.spookystuff.{ActionException, Const, SpookyContext}
import org.apache.spark.ml.dsl.utils._
import org.apache.spark.sql.types.SQLUserDefinedType
import org.json4s.Formats
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

class ActionUDT extends ScalaUDT[Action]

object ActionRelay extends MessageRelay[Action] {

  //  override implicit def formats: Formats = Xml.defaultFormats + FallbackJSONSerializer

  def batchConvert(elements: Traversable[_]): Traversable[Any] = elements
    .map {
      v =>
        convert(v)
    }

  private def convert(value: Any) = {
    value match {
      case v: HasMessage => v.toMessageValue
      case (k, v: HasMessage) => k -> v.toMessageValue
      case v: Traversable[_] => batchConvert(v)
      case v => v
    }
  }

  //avoid using scala reflections on worker as they are thread unsafe, use JSON4s that is more battle tested
  override def toMessage(value: Action): M = {
    val className = value.getClass.getCanonicalName
    val map: Map[String, Any] = Map(SpookyUtils.Reflection.getCaseAccessorMap(value): _*)
    val effectiveMap = map.mapValues {convert}

    M(
      className,
      effectiveMap
    )
  }

  //TODO: change to MessageRepr to allow 2-way conversions.
  case class M(
                className: String,
                params: Map[String, Any]
              ) extends Message {

    override def formats: Formats = ActionRelay.this.formats
  }
}

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
trait Action extends ActionLike with ActionRelay.HasRelay{

  override def children: Trace = Nil

  var timeElapsed: Long = -1 //only set once

  override def dryrun: List[List[Action]] = {
    if (hasOutput){
      List(List(this))
    }
    else {
      List()
    }
  }

  //this should handle autoSave, cache and errorDump
  override def apply(session: Session): Seq[Fetched] = {

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
    session.spooky.metrics.pagesFetchedFromRemote += results.count(_.isInstanceOf[Doc])

    results
  }

  //execute errorDumps as side effects
  protected def getSessionExceptionString(
                                           session: Session,
                                           docOpt: Option[Doc] = None
                                         ): String = {
    var message: String = "\n{\n"

    message += {
      session.backtrace.map {
        action =>
          "| " + action.toString
      } ++
        Seq("+> " + this.toStringVerbose)
    }
      .mkString("\n")

    val errorDump: Boolean = session.spooky.conf.errorDump
    val errorDumpScreenshot: Boolean = session.spooky.conf.errorScreenshot

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
              val rawPage = ErrorScreenshot.exe(session).toList.head.asInstanceOf[Doc]
              message += "\nScreenshot: " + this.errorDump(message, rawPage, session.spooky)
            }
            catch {
              case e: Throwable =>
                LoggerFactory.getLogger(this.getClass).error("Cannot take screenshot on ActionError:", e)
            }
          }
        }
        else {
          docOpt.foreach {
            doc =>
              if (errorDump) {
                message += "\nSnapshot: " + this.errorDump(message, doc, session.spooky)
              }
          }
        }
    }
    message
  }

  protected def errorDump(message: String, rawPage: Doc, spooky: SpookyContext): String = {

    val backtrace = if (rawPage.uid.backtrace.lastOption.exists(_ eq this)) rawPage.uid.backtrace
    else rawPage.uid.backtrace :+ this
    val uid = rawPage.uid.copy(backtrace = backtrace)(name = null)
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

  protected[actions] def withDriversTimedDuring[T](session: Session)(f: => T) = {

    this match {
      case tt: Timed =>
        LoggerFactory.getLogger(this.getClass).info(s"+> ${this.toStringVerbose} in ${tt.timeout(session)}")

        session.withDriversDuring(
          SpookyUtils.withDeadline(tt.hardTerminateTimeout(session)) {
            f
          }
        )
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(s"+> ${this.toString}")

        session.withDriversDuring(
          f
        )
    }
  }

  protected[actions] def exe(session: Session): Seq[Fetched] = {
    withDriversTimedDuring(session){
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

  var _timeout: Duration = _

  def in(deadline: Duration): this.type = {
    this._timeout = deadline
    this
  }

  def timeout(session: Session): Duration = {
    val base = if (this._timeout == null) session.spooky.conf.remoteResourceTimeout
    else this._timeout

    base
  }

  //TODO: this causes downloading large files to fail, need a better mechanism
  def hardTerminateTimeout(session: Session): Duration = {
    timeout(session) + Const.hardTerminateOverhead
  }

  def webDriverWait(session: Session): WebDriverWait = new WebDriverWait(session.webDriver, this.timeout(session).toSeconds)

  def getClickableElement(selector: String, session: Session) = {

    val elements = webDriverWait(session).until(ExpectedConditions.elementToBeClickable(new BySizzleCssSelector(selector)))

    elements
  }

  def getElement(selector: String, session: Session) = {

    val elements = webDriverWait(session).until(ExpectedConditions.presenceOfElementLocated(new BySizzleCssSelector(selector)))

    elements
  }

  def getElements(selector: String, session: Session) = {

    val elements = webDriverWait(session).until(ExpectedConditions.presenceOfAllElementsLocatedBy(new BySizzleCssSelector(selector)))

    elements
  }

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this._timeout = same.asInstanceOf[Timed]._timeout
  }
}

trait Named extends Action {

  var nameOpt: Option[String] = None
  def name = nameOpt.getOrElse(this.toString)

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

trait Driverless extends Action {
}