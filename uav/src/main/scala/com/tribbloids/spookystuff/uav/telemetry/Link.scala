package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.caching.Memoize
import com.tribbloids.spookystuff.session.python.PyRef
import com.tribbloids.spookystuff.session.{Cleanable, Lifespan, ResourceLedger, Session}
import com.tribbloids.spookystuff.uav.dsl.LinkFactory
import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.system.Drone
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.uav.{ReinforcementDepletedException, UAVConf}
import com.tribbloids.spookystuff.utils.{NOTSerializable, SpookyUtils, TreeException}
import com.tribbloids.spookystuff.{SpookyContext, caching}
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
                 executedBy: Seq[Drone],
                 session: Session,
                 selector: Seq[Link] => Option[Link] = {
                   vs =>
                     vs.headOption
                 },
                 recommission: Boolean = true
               ): Try[Link] = {

    val sessionLifespanOpt = Some(session.lifespan)

    val localOpt = {
      val locals = Link.existing.values.toList.filter{
        v =>
          val l1Opt = v.usedInLifespanOpt
          val l2Opt = sessionLifespanOpt
          val lMatch = (l1Opt, l2Opt) match {
            case (Some(tc1), Some(tc2)) if tc1 == tc2 => true
            case _ => false
          }
          lMatch
      }
      assert(locals.size <= 1, "Multiple Links cannot share task context or thread")
      val result = locals.headOption
      result.foreach(_.usedInLifespan = session.lifespan)
      result
    }

    var resultOpt = localOpt.orElse {
      val links = executedBy.map(_.getLink(session.spooky))

      this.synchronized {
        val available = links.filter(v => v.isAvailable)
        val selectedOpt = selector(available)
        selectedOpt.foreach(_.usedInLifespan = session.lifespan)
        selectedOpt
      }
    }

    resultOpt match {
      case Some(link) =>
        if (recommission) {
          val factory = session.spooky.submodule[UAVConf].linkFactory
          Success(link.recommission(factory))
        }
        else {
          Success(link)
        }
      case None =>
        val info = if (Link.existing.isEmpty) {
          val msg = s"No telemetry Link for ${executedBy.mkString("[", ", ", "]")}, existing links are:"
          val hint = Link.existing.keys.toList.mkString("[", ", ", "]")
          msg + "\n" + hint
        }
        else {
          "All telemetry Links are busy:\n" +
            Link.existing.values.map {
              link =>
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

  @volatile protected var _usedInLifespan: Lifespan = _
  def usedInLifespanOpt = Option(_usedInLifespan)
  def usedInLifespan = usedInLifespanOpt.get
  def usedInLifespan_=(
                        c: Lifespan
                      ): this.type = {

    if (usedInLifespanOpt.exists(_ == c)) this
    else {
      this.synchronized{
        assert(isNotUsedInLifespan, s"Cannot set usedInLifespan to $c: $statusString")
        this._usedInLifespan = c
        this
      }
    }
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
  @volatile var isNotBooked: Boolean = true

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

  def isNotUsedInLifespan: Boolean = {
    Option(_usedInLifespan).forall(_.isTerminated)
  }

  def isAvailable: Boolean = {
    isNotBooked && isNotBlacklisted && isNotUsedInLifespan
  }

  def statusString: String = {

    val strs = ArrayBuffer[String]()
    if (!isNotBooked)
      strs += "booked"
    if (!isNotBlacklisted)
      strs += s"unreachable for ${(System.currentTimeMillis() - lastFailureOpt.get._2).toDouble / 1000}s" +
        s" (${lastFailureOpt.get._1.getClass.getSimpleName})"
    if (!isNotUsedInLifespan)
      strs += s"used by ${_usedInLifespan}"

    s"Link $drone is " + {
      if (isAvailable) {
        assert(strs.isEmpty)
        "available"
      }
      else {
        strs.mkString(" & ")
      }
    }
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
        s"Recreating link with new factory ${factory.getClass.getSimpleName}"
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

  // Most telemetry support setting up multiple landing site.
  protected def _getHome: Location
  final lazy val home: Location = {
    retry(5){
      _getHome
    }
  }

  protected def _getCurrentLocation: Location
  object CurrentLocation extends Memoize[Unit, Location]{
    override def f(v: Unit): Location = {
      retry(5) {
        _getCurrentLocation
      }
    }
  }
  def currentLocation(expireAfter: Long = 1000): Location = {
    CurrentLocation.getIfNotExpire((), expireAfter)
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