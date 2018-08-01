package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.caching.Memoize
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.uav.dsl.Routing
import com.tribbloids.spookystuff.uav.spatial.point.Location
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.utils.{Lock, UAVUtils}
import com.tribbloids.spookystuff.uav.{UAVConf, UAVMetrics}
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, CommonUtils, TreeException}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.{Failure, Try}

/**
  * Created by peng on 24/01/17.
  */
object Link {

  // not all created Links are registered
  val registered: CachingUtils.ConcurrentMap[UAV, Link] = CachingUtils.ConcurrentMap()

  def statusStrs: List[String] ={
    registered.values.toList.map(_.statusStr)
  }

  def sanityCheck(): Unit = {
    val cLinks = Cleanable.getTyped[Link].toSet
    val rLinks = registered.values.toSet
    val residual = rLinks -- cLinks
    assert (
      residual.isEmpty,
      s"the following link(s) are registered but not cleanable:\n" +
        residual.map(v => v.logPrefix + v.toString).mkString("\n")
    )
  }
}

trait Link extends LocalCleanable with ConflictDetection {

  val uav: UAV

  val resourceURIs: Set[String]
  final override lazy val _resourceIDs = Map("uris" -> resourceURIs)

  override def _lifespan = new Lifespan.JVM(
    nameOpt = Some(this.getClass.getSimpleName)
  )


  @volatile protected var _spooky: SpookyContext = _
  def spookyOpt = Option(_spooky)

  @volatile protected var _factory: Routing = _
  def factoryOpt = Option(_factory)

  lazy val runOnce: Unit = {
    spookyOpt.get.getMetrics[UAVMetrics].linkCreated += 1
  }

  def register(
                spooky: SpookyContext = this._spooky,
                factory: Routing = this._factory
              ): this.type = Link.synchronized{

    try {
      _spooky = spooky
      _factory = factory
      //      _taskContext = taskContext
      spookyOpt.foreach(_ => runOnce)

      val inserted = Link.registered.getOrElseUpdate(uav, this)
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
  def isRegistered: Boolean = Link.registered.get(uav).contains(this)

  //  @volatile protected var _owner: LifespanContext = _ //TODO: merge into lock
  //  def ownerOpt = Option(_owner)
  //  def owner = ownerOpt.get
  //  def owner_=(c: LifespanContext): this.type = {
  //
  //    if (ownerOpt.contains(c)) this
  //    else {
  //      this.synchronized{
  //        assert(
  //          isNotOwned,
  //          s"Unavailable to ${c.toString} until owner thread/task is completed:\n$statusStr"
  //        )
  //        this._owner = c
  //        this
  //      }
  //    }
  //  }
  //  def isOwnedBy(v: LifespanContext) = ownerOpt.exists(vv => v._id == vv._id)
  //  def isNotOwned: Boolean = ownerOpt.forall(v => v.isCompleted)

  // IMPORTANT: ALWAYS set owner first!
  // or the link may become available to other threads and snatched by them
  //  def setOwnerAndUnlock(ctx: LifespanContext, validate: Boolean = true): Unit = {
  //    owner = ctx
  //    if (validate) assert(!isNotOwned, "IMPOSSIBLE!")
  //    unlock()
  //  }

  //finalizer may kick in and invoke it even if its in Link.existing
  override protected def cleanImpl(): Unit = {

    val existingOpt = Link.registered.get(uav)
    existingOpt.foreach {
      v =>
        if (v eq this)
          Link.registered -= uav
        else {
          val registeredThis = Link.registered.toList.filter(_._2 eq this)
          assert(
            registeredThis.isEmpty,
            {
              s"""
                 |this link's UAV is registered with a different link:
                 |$uav -> ${Link.registered(uav)}
                 |====================================================
                 |and this link is registered with a diffferent UAV:
                 |${registeredThis.map{
                tuple =>
                  s"${tuple._1} -> ${tuple._2}"
              }.mkString("\n")
              }
              """.stripMargin
            }
          )
        }
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
        spooky.getConf[UAVConf].connectionRetries
    )
    .getOrElse(UAVConf.FAST_CONNECTION_RETRIES)

  @volatile var lastFailureOpt: Option[(Throwable, Long)] = None

  protected def detectConflicts(): Unit = {
    val notMe: Seq[Link] = Link.registered.values.toList.filterNot(_ eq this)

    for (
      myURI <- this.resourceURIs;
      notMe1 <- notMe
    ) {
      val notMyURIs = notMe1.resourceURIs
      assert(!notMyURIs.contains(myURI), s"'$myURI' is already used by link ${notMe1.uav}")
    }
  }

  /**
    * A utility function that all implementation should ideally be enclosed
    * Telemetry are inheritively unstable, so its better to reconnect if anything goes wrong.
    * after all retries are exhausted will try to detect URL conflict and give a report as informative as possible.
    */
  def withConn[T](n: Int = connectRetries, interval: Long = 0, silent: Boolean = false)(
    fn: =>T
  ): T = {
    try {
      CommonUtils.retry(n, interval, silent) {
        try {
          connectIfNot()
          fn
        }
        catch {
          case e: Throwable =>
            disconnect()
            val sanityTrials = Seq(Failure[Unit](e)) ++
              Seq(Try(detectConflicts())) ++
              UAVUtils.localSanityTrials
            val afterDetection = {
              try {
                TreeException.&&&(sanityTrials)
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
    * set this to avoid being used by another task even the current task finish.
    */
  @volatile var _lock: Lock = _
  def lock: Lock = Option(_lock).getOrElse(Lock.Open)
  def lock_=(v: Lock): Unit = this.synchronized{
    assert(lock.getAvailability(Some(v)) >= 0,
      s"Cannot lock to $v until existing lock is opened:\n$statusStr"
    )
    this._lock = v
  }
  def unlock(): Unit = {
    _lock = null
  }
  //  def isLocked: Boolean = lockOpt.exists(v => System.currentTimeMillis() < v.expireAfter)
  //  def isLockedBy(lockID: UUID): Boolean = {
  //    lockOpt.map(_._id).contains(lockID)
  //  }

  private def blacklistDuration: Long = spookyOpt
    .map(
      spooky =>
        spooky.getConf[UAVConf].blacklistResetAfter
    )
    .getOrElse(UAVConf.BLACKLIST_RESET_AFTER)
    .toMillis

  def isReachable: Boolean = !lastFailureOpt.exists {
    tt =>
      System.currentTimeMillis() - tt._2 <= blacklistDuration
  }

  //  def isAvailable: Boolean = {
  //    !isLocked && isReachable && isNotOwned && !isCleaned
  //  }

  // return true regardless if given the same MutexID
  def isAvailable(key: Option[Lock] = None): Boolean = {
    isReachable && !isCleaned && (lock.getAvailability(key) >= 0)
  }

  def statusStr: String = {

    val parts = ArrayBuffer[String]()
    if (!isReachable)
      parts += s"unreachable for ${(System.currentTimeMillis() - lastFailureOpt.get._2).toDouble / 1000}s" +
        s" (${lastFailureOpt.get._1.getClass.getSimpleName})"
    if (lock.getAvailability() < 0) parts += s"locked by $lock"
    if (isCleaned) parts += s"already cleaned"

    if (isAvailable()) {
      assert(parts.isEmpty, "INTERNAL ERROR, " + parts.mkString(" & "))
      parts += "available"
    }

    val info = parts.mkString(" & ")

    s"${this.getClass.getSimpleName} $uav -> " + info
  }

  def sameFactoryWith(b: Link): Boolean
  def recommission(
                    factory: Routing
                  ): Link = Link.synchronized {

    val neo: Link = factory.apply(uav)
    neo.lock = this._lock
    val result = if (sameFactoryWith(neo)) {
      LoggerFactory.getLogger(this.getClass).info {
        s"recommissioning existing link for $uav"
      }
      neo.clean()
      this
    }
    else {
      LoggerFactory.getLogger(this.getClass).info {
        s"recommissioning link for $uav with new factory ${factory.getClass.getSimpleName}"
      }
      this.clean()
      neo
    }
    result.register(
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
    withConn()(Unit)
  }

  // Most telemetry support setting up multiple landing site.
  protected def _getHome: Location
  protected def home: Location = {
    withConn(){
      _getHome
    }
  }

  protected def _getCurrentLocation: Location
  protected object CurrentLocation extends Memoize[Unit, Location]{
    override def f(v: Unit): Location = {
      withConn() {
        _getCurrentLocation
      }
    }
  }

  def status(expireAfter: Long = 1000): LinkStatus = {
    val current = CurrentLocation.getIfNotExpire((), expireAfter)
    LinkStatus(uav, lock, home, current)
  }

  //====================== Synchronous API ======================
  // TODO this should be abandoned and mimic by Asynch API

  val synchAPI: SynchronousAPI
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
