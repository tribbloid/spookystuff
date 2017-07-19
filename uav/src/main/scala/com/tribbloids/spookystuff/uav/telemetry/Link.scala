package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.caching.Memoize
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.session.python.PyRef
import com.tribbloids.spookystuff.uav.dsl.LinkFactory
import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.uav.{ReinforcementDepletedException, UAVConf, UAVMetrics}
import com.tribbloids.spookystuff.utils.{SpookyUtils, TreeException}
import com.tribbloids.spookystuff.{SpookyContext, caching}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
  * Created by peng on 24/01/17.
  */
object Link {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  // connStr -> (link, isBusy)
  // only 1 allowed per connStr, how to enforce?
  val existing: caching.ConcurrentMap[UAV, Link] = caching.ConcurrentMap()

  def select(
              uavs: Seq[UAV],
              session: Session,
              prefer: Seq[Link] => Option[Link] = {
                vs =>
                  vs.headOption
              },
              recommissionWithNewProxy: Boolean = true
            ): Link = {

    trySelect(uavs, session, prefer, recommissionWithNewProxy).get
  }

  def trySelect(
                 uavs: Seq[UAV],
                 session: Session,
                 prefer: Seq[Link] => Option[Link] = {
                   vs =>
                     vs.headOption
                 },
                 recommissionWithNewProxy: Boolean = true
               ) = Try {

    SpookyUtils.retry(3, 1000) {
      _trySelect(uavs, session, prefer, recommissionWithNewProxy).get
    }
  }

  def _trySelect(
                  uavs: Seq[UAV],
                  session: Session,
                  prefer: Seq[Link] => Option[Link] = {
                    vs =>
                      vs.headOption
                  },
                  recommissionWithNewProxy: Boolean = true
                ): Try[Link] = {

    val sessionThreadOpt = Some(session.lifespan.ctx.thread)

    val threadLocalOpt = {
      val id2Opt = sessionThreadOpt.map(_.getId)
      val results = Link.existing.values.toList.filter {
        v =>
          val id1Opt = v.usedByOpt.map(_.thread.getId)
          val lMatch = (id1Opt, id2Opt) match {
            case (Some(tc1), Some(tc2)) if tc1 == tc2 => true
            case _ => false
          }
          lMatch
      }
      assert(results.size <= 1, "Multiple Links cannot share task context or thread")
      val opt = results.headOption
//      opt.foreach(_.usedBy = session.lifespan.ctx)
      opt
    }

    val recommissionedOpt = threadLocalOpt match {
      case None =>
        LoggerFactory.getLogger(this.getClass).info (
          s"ThreadLocal link not found: ${session.lifespan.ctx.toString}" +
            s" <\\- ${Link.existing.values.flatMap(_.usedByOpt)
              .mkString("{", ", ", "}")}"
        )
        None
      case Some(threadLocal) =>
        val v = if (recommissionWithNewProxy) {
          val factory = session.spooky.getConf[UAVConf].linkFactory
          threadLocal.recommission(factory)
        }
        else {
          threadLocal
        }
        Try{
          v.connect()
          v
        }
          .toOption
    }

    // no need to recommission if the link is fre
    val resultOpt = recommissionedOpt
      .orElse {
        val links = uavs.flatMap{
          uav =>
            val vv = uav.getLink(session.spooky)
            Try {
              vv.connect()
              vv
            }
              .toOption
        }

        val opt = this.synchronized {
          val available = links.filter(v => v.isAvailable)
          val opt = prefer(available)
          //deliberately set inside synchronized block to avoid being selected by 2 threads
          opt.foreach(_.usedBy = session.lifespan.ctx)
          opt
        }

        opt
      }

    resultOpt.foreach(_.usedBy = session.lifespan.ctx)

    resultOpt match {
      case Some(link) =>
        Success {
          link
        }
      case None =>
        val info = if (Link.existing.isEmpty) {
          val msg = s"No telemetry Link for ${uavs.mkString("[", ", ", "]")}, existing links are:"
          val hint = Link.existing.keys.toList.mkString("[", ", ", "]")
          msg + "\n" + hint
        }
        else {
          "All telemetry links are busy:\n" +
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

  def allConflicts: Seq[Try[Unit]] = {
    Seq(
      Try(PyRef.sanityCheck()),
      Try(MAVLink.sanityCheck())
    ) ++
      ConflictDetection.conflicts
  }

  // get available drones, TODO: merge other impl to it.
  def availableLinkRDD(spooky: SpookyContext): RDD[(UAVStatus, Link)] = {

    val rdd = spooky.sparkContext
      .mapPerExecutorCore({
        spooky.withSession {
          session =>
            val uavsInFleet = spooky.getConf[UAVConf].uavsInFleetShuffled
            val linkTry = Link.trySelect (
              uavsInFleet,
              session
            )
            linkTry.map {
              link =>
                link.status() -> link
            }
        }
      })
    rdd.flatMap {
      v =>
        v.recover {
          case e =>
            LoggerFactory.getLogger(this.getClass).warn(e.toString)
            throw e
        }
          .toOption
    }
  }
}

trait Link extends LocalCleanable with ConflictDetection {

  val uav: UAV

  val exclusiveURIs: Set[String]
  final override lazy val resourceIDs = Map("uris" -> exclusiveURIs)

  @volatile protected var _spooky: SpookyContext = _
  def spookyOpt = Option(_spooky)

  @volatile protected var _factory: LinkFactory = _
  def factoryOpt = Option(_factory)

  lazy val runOnce: Unit = {
    spookyOpt.get.getMetrics[UAVMetrics].linkCreated += 1
  }

  def setFactory(
                  spooky: SpookyContext = this._spooky,
                  factory: LinkFactory = this._factory
                ): this.type = Link.synchronized{

    try {
      _spooky = spooky
      _factory = factory
      //      _taskContext = taskContext
      spookyOpt.foreach(_ => runOnce)

      val inserted = Link.existing.getOrElseUpdate(uav, this)
      assert(
        inserted eq this,
        {
          s"Multiple Links created for UAV $uav"
        }
      )

      this
    }
    catch {
      case e: Throwable =>
        this.clean()
        throw e
    }
  }

  @volatile protected var _usedBy: LifespanContext = _
  def usedByOpt = Option(_usedBy)
  def usedBy = usedByOpt.get
  def usedBy_= (
                 c: LifespanContext
               ): this.type = {

    if (usedByOpt.exists(v => v == c)) this
    else {

      this.synchronized{
        assert(
          isNotUsed,
          s"Unavailable to ${c.toString} until current thread/task is finished: $statusString"
        )
        this._usedBy = c
        this
      }
    }
  }

  //finalizer may kick in and invoke it even if its in Link.existing
  override protected def cleanImpl(): Unit = {

    val existingOpt = Link.existing.get(uav)
    existingOpt.foreach {
      v =>
        if (v eq this)
          Link.existing -= uav
    }
    spookyOpt.foreach {
      spooky =>
        spooky.getMetrics[UAVMetrics].linkDestroyed += 1
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
        spooky.getConf[UAVConf].fastConnectionRetries
    )
    .getOrElse(UAVConf.FAST_CONNECTION_RETRIES)

  @volatile var lastFailureOpt: Option[(Throwable, Long)] = None

  protected def detectConflicts(): Unit = {
    val notMe: Seq[Link] = Link.existing.values.toList.filterNot(_ eq this)

    for (
      myURI <- this.exclusiveURIs;
      notMe1 <- notMe
    ) {
      val notMyURIs = notMe1.exclusiveURIs
      assert(!notMyURIs.contains(myURI), s"'$myURI' is already used by link ${notMe1.uav}")
    }
  }

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
            if (!silent) LoggerFactory.getLogger(this.getClass).warn(s"CONNECTION TO $uav FAILED!", afterDetection)
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
    * set true to block being used by another thread
    * This should be a Future to indicate 3 states:
    * Completed(true): don't think about it, just use another link or (if depleted) report error.
    * Completed(false): use it immediately.
    * Pending: wait for result, then decide.
    */
  @volatile var isBooked: Boolean = false

  private def blacklistDuration: Long = spookyOpt
    .map(
      spooky =>
        spooky.getConf[UAVConf].slowConnectionRetryInterval
    )
    .getOrElse(UAVConf.BLACKLIST_RESET_AFTER)
    .toMillis

  def isReachable: Boolean = !lastFailureOpt.exists {
    tt =>
      System.currentTimeMillis() - tt._2 <= blacklistDuration
  }

  def isNotUsedByThread: Boolean = {
    usedByOpt.forall(v => !v.thread.isAlive)
  }
  def isNotUsedByTask: Boolean = {
    usedByOpt.flatMap(_.taskContextOpt).forall(v => v.isCompleted())
  }
  def isNotUsed = isNotUsedByThread || isNotUsedByTask

  def isAvailable: Boolean = {
    !isBooked && isReachable && isNotUsed
  }
  //
//  def maybeAvailable: Boolean = {
//    !isBooked && isReachable && isNotUsedByTask
//  }

  def statusString: String = {

    val strs = ArrayBuffer[String]()
    if (isBooked)
      strs += "booked"
    if (!isReachable)
      strs += s"unreachable for ${(System.currentTimeMillis() - lastFailureOpt.get._2).toDouble / 1000}s" +
        s" (${lastFailureOpt.get._1.getClass.getSimpleName})"
    if (!isNotUsedByThread || !isNotUsedByTask)
      strs += s"used by ${_usedBy.toString}"

    s"Link $uav is " + {
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

    val neo = factory.apply(uav)
    val result = if (coFactory(neo)) {
      LoggerFactory.getLogger(this.getClass).info {
        s"Reusing existing link for $uav"
      }
      neo.clean(silent = true)
      this
    }
    else {
      LoggerFactory.getLogger(this.getClass).info {
        s"Recreating link for $uav with new factory ${factory.getClass.getSimpleName}"
      }
      this.clean(silent = true)
      neo
    }
    result.setFactory(
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
  protected lazy val getHome: Location = {
    retry(){
      _getHome
    }
  }

  protected def _getCurrentLocation: Location
  protected object CurrentLocation extends Memoize[Unit, Location]{
    override def f(v: Unit): Location = {
      retry() {
        _getCurrentLocation
      }
    }
  }

  def status(expireAfter: Long = 1000): UAVStatus = {
    val current = CurrentLocation.getIfNotExpire((), expireAfter)
    UAVStatus(uav, getHome, current)
  }

  //====================== Synchronous API ======================
  // TODO this should be abandoned and mimic by Asynch API

  val synch: SynchronousAPI
  abstract class SynchronousAPI {
    def testMove: String

    def clearanceAlt(alt: Double): Unit
    def goto(location: Location): Unit
  }

  //====================== Asynchronous API =====================

  //  val Asynch: AsynchronousAPI
  //  abstract class AsynchronousAPI {
  //    def move(): Unit
  //  }
}
