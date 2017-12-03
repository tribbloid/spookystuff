package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.caching.Memoize
import com.tribbloids.spookystuff.session._
import com.tribbloids.spookystuff.uav.dsl.LinkFactory
import com.tribbloids.spookystuff.uav.spatial.point.Location
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.Link.MutexLock
import com.tribbloids.spookystuff.uav.utils.UAVUtils
import com.tribbloids.spookystuff.uav.{ReinforcementDepletedException, UAVConf, UAVMetrics}
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, LifespanContext, LocalCleanable}
import com.tribbloids.spookystuff.utils.{CachingUtils, CommonUtils, IDMixin, TreeException}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.{Failure, Random, Success, Try}

/**
  * Created by peng on 24/01/17.
  */
object Link {

  // not all created Links are registered
  val registered: CachingUtils.ConcurrentMap[UAV, Link] = CachingUtils.ConcurrentMap()

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

  val LOCK_EXPIRE_AFTER = 60 * 1000
  case class MutexLock(
                        _id: Long = Random.nextLong(), //can only be lifted by PreferUAV that has the same token.
                        timestamp: Long = System.currentTimeMillis()
                      ) extends IDMixin {

    def expireAfter = timestamp + LOCK_EXPIRE_AFTER
  }

  case class Selector(
                       fleet: Seq[UAV],
                       session: Session,
                       prefer: Seq[Link] => Option[Link] = {
                         vs =>
                           vs.find(_.isAvailable)
                       },
                       recommissionWithNewProxy: Boolean = true
                     ) {

    lazy val spooky = session.spooky
    lazy val ctx = session.lifespan.ctx

    def select: Link = {

      trySelect.get
    }

    def trySelect: Try[Link] = Try {

      CommonUtils.retry(3, 1000) {
        _trySelect.get
      }
    }

    /**
      * @return a link that is unlocked and owned by session's lifespan
      */
    private def _trySelect: Try[Link] = {

      val threadOpt = Some(ctx.thread)

      // IMPORTANT: ALWAYS set owner first!
      // or the link may become available to other threads and snatched by them
      def setOwnerAndUnlock(v: Link): Link = {
        v.owner = ctx
        v.unlock()
        v
      }

      val threadLocalOpt = {
        val id2Opt = threadOpt.map(_.getId)
        val local = Link.registered.values.toList.filter {
          v =>
            val id1Opt = v.ownerOpt.map(_.thread.getId)
            val lMatch = (id1Opt, id2Opt) match {
              case (Some(tc1), Some(tc2)) if tc1 == tc2 => true
              case _ => false
            }
            lMatch
        }

        assert(
          local.size <= 1,
          s"""
             |Multiple Links cannot share task context or thread:
             |${local.map(_.status()).mkString("\n")}
          """.stripMargin
        )
        val opt = local.find {
          link =>
            fleet.contains(link.uav)
        }
        opt.map(setOwnerAndUnlock)
      }

      val recommissionedOpt = threadLocalOpt match {
        case None =>
          LoggerFactory.getLogger(this.getClass).info (
            s"${ctx.toString} ThreadLocal link not found:\n" +
              Link.registered.values.flatMap(_.ownerOpt)
                .mkString("\n")
          )
          None
        case Some(threadLocal) =>
          val v = if (recommissionWithNewProxy) {
            val factory = spooky.getConf[UAVConf].linkFactory
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

      // no need to recommission if the link is free
      val resultOpt = recommissionedOpt
        .orElse {
          val links = fleet.flatMap{
            uav =>
              val vv = uav.getLink(spooky)
              Try {
                vv.connect()
                vv
              }
                .toOption
          }

          val opt = Link.synchronized {
            val opt = prefer(links)
            //deliberately set inside synched block to avoid being selected by 2 threads
            opt.map(setOwnerAndUnlock)
          }

          opt
        }

      resultOpt match {
        case Some(link) =>
          assert(
            link.owner == ctx,
            s"owner inconsistent! ${link.owner} != $ctx"
          )
          Success {
            link
          }
        case None =>
          val info = if (Link.registered.isEmpty) {
            val msg = s"No telemetry Link for ${fleet.mkString("[", ", ", "]")}, existing links are:"
            val hint = Link.registered.keys.toList.mkString("[", ", ", "]")
            msg + "\n" + hint
          }
          else {
            "All telemetry links are busy:\n" +
              Link.registered.values.map {
                link =>
                  link.statusString
              }
                .mkString("\n")
          }
          Failure(
            new ReinforcementDepletedException(ctx.toString + " " +info)
          )
      }
    }
  }

  object Selector {

    def withMutex(
                   fleet: Seq[UAV],
                   session: Session,
                   mutexIDOpt: Option[Long]
                 ) = Selector(
      fleet,
      session,
      prefer = {
        vs =>
          vs.find(_.isAvailableTo(mutexIDOpt))
      }
    )
  }
}

trait Link extends LocalCleanable with ConflictDetection {

  val uav: UAV

  val exclusiveURIs: Set[String]
  final override lazy val _resourceIDs = Map("uris" -> exclusiveURIs)

  @volatile protected var _spooky: SpookyContext = _
  def spookyOpt = Option(_spooky)

  @volatile protected var _factory: LinkFactory = _
  def factoryOpt = Option(_factory)

  lazy val runOnce: Unit = {
    spookyOpt.get.getMetrics[UAVMetrics].linkCreated += 1
  }

  def register(
                spooky: SpookyContext = this._spooky,
                factory: LinkFactory = this._factory
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
  def isRegistered: Boolean = Link.registered.get(uav) == Some(this)

  @volatile protected var _owner: LifespanContext = _
  def ownerOpt = Option(_owner)
  def owner = ownerOpt.get
  def owner_=(
               c: LifespanContext
             ): this.type = {

    if (ownerOpt.exists(v => v == c)) this
    else {
      this.synchronized{
        assert(
          isNotUsed,
          s"Unavailable to ${c.toString} until previous thread/task is finished: $statusString"
        )
        this._owner = c
        this
      }
    }
  }

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
        spooky.getConf[UAVConf].fastConnectionRetries
    )
    .getOrElse(UAVConf.FAST_CONNECTION_RETRIES)

  @volatile var lastFailureOpt: Option[(Throwable, Long)] = None

  protected def detectConflicts(): Unit = {
    val notMe: Seq[Link] = Link.registered.values.toList.filterNot(_ eq this)

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
  @volatile var _mutex: Option[MutexLock] = None
  def isLocked: Boolean = _mutex.exists(v => System.currentTimeMillis() < v.expireAfter)
  def lock(): MutexLock = {
    assert(!isLocked)
    val v = MutexLock()
    _mutex = Some(v)
    v
  }
  def unlock(): Unit = {
    _mutex = None
  }

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
    ownerOpt.forall(v => !v.thread.isAlive)
  }
  def isNotUsedByTask: Boolean = {
    ownerOpt.flatMap(_.taskOpt).forall(v => v.isCompleted())
  }
  def isNotUsed = isNotUsedByThread || isNotUsedByTask

  def isAvailable: Boolean = {
    !isLocked && isReachable && isNotUsed && !isCleaned
  }
  // return true regardless if given the same MutexID
  def isAvailableTo(mutexIDOpt: Option[Long]): Boolean = {
    isAvailable || {
      mutexIDOpt.exists(
        mutexID =>
          _mutex.get._id == mutexID
      )
    }
  }

  // TODO: move to UAVStatus?
  def statusString: String = {

    val strs = ArrayBuffer[String]()
    if (isLocked)
      strs += "locked"
    if (!isReachable)
      strs += s"unreachable for ${(System.currentTimeMillis() - lastFailureOpt.get._2).toDouble / 1000}s" +
        s" (${lastFailureOpt.get._1.getClass.getSimpleName})"
    if (!isNotUsedByThread || !isNotUsedByTask)
      strs += s"used by ${_owner.toString}"

    s"${this.getClass.getSimpleName} $uav is " + {
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
                  ): Link = Link.synchronized {

    val neo = factory.apply(uav)
    val result = if (coFactory(neo)) {
      LoggerFactory.getLogger(this.getClass).info {
        s"Reusing existing link for $uav"
      }
      neo.clean()
      this
    }
    else {
      LoggerFactory.getLogger(this.getClass).info {
        s"Recreating link for $uav with new factory ${factory.getClass.getSimpleName}"
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
    UAVStatus(uav, ownerOpt, getHome, current)
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
