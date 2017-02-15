package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.session.python.PyRef
import com.tribbloids.spookystuff.session.{Cleanable, ResourceLedger, Session}
import com.tribbloids.spookystuff.uav.dsl.LinkFactory
import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.system.Drone
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.uav.{ReinforcementDepletedException, UAVConf}
import com.tribbloids.spookystuff.utils.{NOTSerializable, SpookyUtils, TreeException}
import com.tribbloids.spookystuff.{SpookyContext, caching}
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
  * Created by peng on 24/01/17.
  */
object Link {

  // connStr -> (link, isBusy)
  // only 1 allowed per connStr, how to enforce?
  val existing: caching.ConcurrentMap[Drone, Link] = caching.ConcurrentMap()

  def trySelect(
                 executedBy: List[Drone],
                 session: Session,
                 selector: Seq[Link] => Option[Link] = {
                   vs =>
                     vs.headOption
                 },
                 recommission: Boolean = true
               ): Try[Link] = {

    val taskLocalOpt = session.taskContextOpt.flatMap {
      tc =>
        val sameTCs = Link.existing.values.toList.filter(_.taskContextOpt == Some(tc))
        assert(sameTCs.size <= 1, "2 Links in 1 task context!")
        sameTCs.headOption
    }

    var resultOpt = taskLocalOpt.orElse {
      val links = executedBy.map(_.toLink(session.spooky))

      this.synchronized {
        val available = links.filter(v => v.isAvailable)
        val selectedOpt = selector(available)
        for (
          tc <- session.taskContextOpt;
          selected <- selectedOpt
        ) {
          selected.setTask(tc)
        }
        selectedOpt
      }
    }

    if (recommission) resultOpt = resultOpt.map {
      link =>
        val factory = session.spooky.conf.submodule[UAVConf].linkFactory
        link.recommission(factory)
    }

    resultOpt match {
      case Some(result) => Success(result)
      case None =>
        val info = if (Link.existing.isEmpty) {
          val msg = s"No existing telemetry Link for ${executedBy.mkString("[", ", ", "]")}, existing links are:"
          val hint = Link.existing.keys.toList.mkString("[", ", ", "]")
          msg + "\n" + hint
        }
        else {
          Link.existing.values.map {
            link =>
              assert(!link.isAvailable)
              link.statusString
          }
            .mkString("\n")
        }
        Failure(
          new ReinforcementDepletedException(info)
        )
    }
  }

  def allConflicts = {
    Seq(
      Try(PyRef.sanityCheck()),
      Try(MAVLink.sanityCheck())
    ) ++
      ResourceLedger.conflicts
  }
}

trait Link extends Cleanable with NOTSerializable {

  val drone: Drone

  @volatile protected var _spooky: SpookyContext = _
  def spookyOpt = Option(_spooky)

  @volatile protected var _factory: LinkFactory = _
  def factoryOpt = Option(_factory)

  lazy val runOnce: Unit = {
    spookyOpt.get.metrics.linkCreated += 1
  }

  def setContext(
                  spooky: SpookyContext = this._spooky,
                  factory: LinkFactory = this._factory
                ): this.type = this.synchronized{

    try {
      _spooky = spooky
      _factory = factory
      //      _taskContext = taskContext
      spookyOpt.foreach(v => runOnce)

      val inserted = Link.existing.getOrElseUpdate(drone, this)
      assert(inserted eq this, s"Multiple Links created for drone $drone")

      this
    }
    catch {
      case e: Throwable =>
        this.clean()
        throw e
    }
  }

  @volatile protected var _taskContext: TaskContext = _
  def taskContextOpt = Option(_taskContext)

  def setTask(
               taskContext: TaskContext
             ): this.type = this.synchronized{

    assert(taskContextOpt.forall(_.isCompleted()))
    this._taskContext = taskContext
    this
  }

  var isDryrun = false
  //finalizer may kick in and invoke it even if its in Link.existing
  override protected def cleanImpl(): Unit = {

    val existingOpt = Link.existing.get(drone)
    existingOpt.foreach {
      v =>
        if (v eq this)
          Link.existing -= drone
        else {
          if (!isDryrun) throw new AssertionError("THIS IS NOT A DRYRUN OBJECT! SO ITS CREATED ILLEGALLY!")
        }
    }
    spookyOpt.foreach {
      spooky =>
        spooky.metrics.linkDestroyed += 1
    }
  }

  var isConnected: Boolean = false
  final def connectIfNot(): Unit = this.synchronized{
    if (!isConnected) {
      _connect()
    }
    isConnected = true
  }
  protected def _connect(): Unit

  final def disconnect(): Unit = this.synchronized{
    _disconnect()
    isConnected = false
  }
  protected def _disconnect(): Unit

  private def connectRetries: Int = spookyOpt
    .map(
      spooky =>
        spooky.conf.submodule[UAVConf].fastConnectionRetries
    )
    .getOrElse(UAVConf.FAST_CONNECTION_RETRIES)

  @volatile var lastFailureOpt: Option[(Throwable, Long)] = None

  protected def detectConflicts(): Unit = {}

  /**
    * A utility function that all implementation should ideally be enclosed
    * All telemetry are inheritively unstable, so its better to reconnect if anything goes wrong.
    * after all retries are exhausted will try to detect URL conflict and give a report as informative as possible.
    */
  def retry[T](n: Int = connectRetries, interval: Long = 0, silent: Boolean = false)(
    fn: =>T
  ): T = {
    try {
      SpookyUtils.retry(n, interval, silent) {
        try {
          connectIfNot()
          fn
        }
        catch {
          case e: Throwable =>
            disconnect()
            val conflicts = Seq(Failure[Unit](e)) ++
              Seq(Try(detectConflicts())) ++
              Link.allConflicts
            val afterDetection = {
              try {
                TreeException.&&&(conflicts)
                e
              }
              catch {
                case ee: Throwable =>
                  ee
              }
            }
            if (!silent) LoggerFactory.getLogger(this.getClass).warn(s"CONNECTION TO $drone FAILED!", afterDetection)
            throw afterDetection
        }
      }
    }
    catch {
      case e: Throwable =>
        lastFailureOpt = Some(e -> System.currentTimeMillis())
        throw e
    }
  }

  /**
    * set true to block being used by another thread before its driver is created
    */
  @volatile var isIdle: Boolean = true

  private def blacklistDuration: Long = spookyOpt
    .map(
      spooky =>
        spooky.conf.submodule[UAVConf].slowConnectionRetryInterval
    )
    .getOrElse(UAVConf.BLACKLIST_RESET_AFTER)
    .toMillis

  def isNotBlacklisted: Boolean = !lastFailureOpt.exists {
    tt =>
      System.currentTimeMillis() - tt._2 <= blacklistDuration
  }

  def isNotInTask: Boolean = {
    taskContextOpt.forall {
      tc =>
        tc.isCompleted()
    }
  }

  def isAvailable: Boolean = {
    isIdle && isNotBlacklisted && isNotInTask
  }

  def statusString: String = {
    val strs = ArrayBuffer[String]()
    if (!isIdle) strs += "busy"
    if (!isNotBlacklisted) strs += s"unreachable for ${(System.currentTimeMillis() - lastFailureOpt.get._2).toDouble / 1000}s" +
      s" (${lastFailureOpt.get._1.getClass.getSimpleName})"
    if (!isNotInTask) strs += s"occupied by Task-${taskContextOpt.get.taskAttemptId()}"
    s"Link $drone is " + strs.mkString(" & ")
  }

  def coFactory(another: Link): Boolean
  def recommission(
                    factory: LinkFactory
                  ): Link = {

    val neo = factory.apply(drone)
    val result = if (coFactory(neo)) {
      LoggerFactory.getLogger(this.getClass).info {
        s"Reusing existing link for $drone"
      }
      neo.isDryrun = true
      neo.clean(silent = true)
      this
    }
    else {
      LoggerFactory.getLogger(this.getClass).info {
        s"Existing link for $drone is obsolete! recreating with new factory ${factory.getClass.getSimpleName}"
      }
      this.clean(silent = true)
      neo
    }
    result.setContext(
      this._spooky,
      factory
    )
    result
  }

  //================== COMMON API ==================

  // will retry 6 times, try twice for Vehicle.connect() in python, if failed, will restart proxy and try again (3 times).
  // after all attempts failed will stop proxy and add endpoint into blacklist.
  // takes a long time.
  def connect(): Unit = {
    retry()(Unit)
  }

  // TODO useless? Link command can set multiple landing site.
  var _home: Location = _
  protected def _getHome: Location
  final def home = retry(){
    Option(_home).getOrElse {
      val v = _getHome
      _home = v
      v
    }
  }

  protected var _lastLocation: Location = _
  protected def _getLocation: Location
  final def getLocation(refresh: Boolean = true) = {
    if (refresh || _lastLocation == null) {
      retry(){
        val v = _getLocation
        _lastLocation = v
        v
      }
    }
    else {
      _lastLocation
    }
  }

  //====================== Synchronous API ===================== TODO this should be abandoned and mimic by Asynch API

  val synch: SynchronousAPI
  abstract class SynchronousAPI {
    def testMove: String

    def clearanceAlt(alt: Double): Unit
    def move(location: Location): Unit
  }

  //====================== Asynchronous API =====================

  //  val Asynch: AsynchronousAPI
  //  abstract class AsynchronousAPI {
  //    def move(): Unit
  //  }
}
