package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.session.{Cleanable, ConflictDetection}
import com.tribbloids.spookystuff.uav.spatial.point.{LLA, Location}
import com.tribbloids.spookystuff.uav.spatial.Anchors
import org.slf4j.LoggerFactory

import scala.language.implicitConversions

object MAVLink {

  def sanityCheck(): Unit = {
    val subs = Cleanable.getTyped[Endpoint] ++ Cleanable.getTyped[Proxy]
    val refSubs = Cleanable.getTyped[MAVLink].flatMap(_.chainClean)
    assert(
      subs.intersect(refSubs).size <= refSubs.size,
      {
        "INTERNAL ERROR: dangling endpoint or proxy without MAVLink!"
      }
    )
  }
}

/**
Contains 0 or 1 proxy and several endpoints to be used by executor.
GCS:UDP:xxx ------------------------> Proxy:TCP:xxx -> Drone
                                   /
TaskProcess -> Connection:UDP:xx -/
            /
  */
case class MAVLink(
                    uav: UAV,
                    toSpark: Seq[String] = Nil, // cannot have duplicates
                    toGCS: Seq[String] = Nil
                  ) extends Link with ConflictDetection {

  {
    if (toSpark.isEmpty) assert(toGCS.isEmpty, "No endpoint for executor")
  }

  val outs: Seq[String] = toSpark ++ toGCS
  val exclusiveURIs: Set[String] = (uav.uris ++ toSpark).toSet

  /**
    * CAUTION: ALL of them have to be val or lazy val! Or you risk recreating many copies each with its own python!
    * Conflict with each other!
    */
  object Endpoints {
    val direct: Endpoint = {
      Endpoint(uav.uris.head, uav.frame, uav.baudRate, uav.groundSSID, uav.name)
    }
    val executors: Seq[Endpoint] = if (toSpark.isEmpty) {
      Seq(direct)
    }
    else {
      toSpark.map {
        out =>
          direct.copy(uri = out)
      }
    }
    //always initialized in Python when created from companion object
    val primary: Endpoint = executors.head
    val GCSs: Seq[Endpoint] = {
      toGCS.map {
        out =>
          direct.copy(uri = out)
      }
    }
    val all: Seq[Endpoint] = (Seq(direct) ++ executors ++ GCSs).distinct
  }

  //  @volatile private var _proxyOpt: Option[Proxy] = None
  val proxyOpt: Option[Proxy] = {
    val result = if (outs.isEmpty) None
    else {
      val proxy = Proxy(
        Endpoints.direct.uri,
        outs,
        Endpoints.direct.baudRate,
        name = uav.name
      )
      Some(proxy)
    }
    result
  }

  def coFactory(another: Link): Boolean = {
    another match {
      case v: MAVLink =>
        val result = this.toGCS.toSet == v.toGCS.toSet
        if (!result) {
          LoggerFactory.getLogger(this.getClass).info (
            s"""
               |Existing link for $uav is obsolete! Recreating ...
               |output should be routed to GCS(s) ${v.toGCS.mkString("[",", ","]")}
               |but instead existing one routes it to ${toGCS.mkString("[",", ","]")}
             """.trim.stripMargin
          )
        }
        result
      case _ =>
        false
    }
  }

  override protected def detectConflicts(): Unit = {
    super.detectConflicts()
    def uri = Endpoints.primary.uri
    val drivers = Cleanable.getTyped[PythonDriver]
    val conflicting = drivers.filter {
      driver =>
        driver.historyCodeOpt.exists(_.contains(uri))
    }
    val myDriver = Option(Endpoints.primary._driver)
    val notMyDriver: Seq[PythonDriver] = conflicting.filterNot {
      driver =>
        myDriver.exists(_ eq driver)
    }
    assert(notMyDriver.isEmpty,
      s"Besides legitimate processs PID=${myDriver.map(_.getPid).orNull}. The following python process(es) also use $uri\n" +
        notMyDriver.map {
          driver =>
            s"=============== PID=${driver.getPid} ===============\n" +
              driver.historyCodeOpt.getOrElse("")
        }
          .mkString("\n")
    )
  }

  override def chainClean: Seq[Cleanable] = {
    Endpoints.all ++
      proxyOpt.toSeq ++
      super.chainClean
  }

  protected def _connect(): Unit = {
    proxyOpt.foreach {
      _.PY.start()
    }
    Endpoints.primary.PY.start()
  }
  protected def _disconnect(): Unit = {
    Endpoints.primary.PYOpt.foreach(_.stop())
    proxyOpt.flatMap(_.PYOpt).foreach {
      _.stop()
    }
  }

  implicit def toPyLocation(
                             p: Location
                           ): PyLocation = {
    val lla = p.getCoordinate(LLA, Anchors.Geodetic).get
    LocationGlobal(lla.lat, lla.lon, lla.alt)
  }

  implicit def fromPyLocation(
                               l: PyLocation
                             ): Location = {
    l match {
      case l: LocationGlobal =>
        LLA(l.lat, l.lon, l.alt) -> Anchors.Geodetic
      case _ =>
        ???
    }
  }

  def _getCurrentLocation: Location = {

    val locations = Endpoints.primary.PY.vehicle.location
    val global = locations.global_frame.$MSG.get.cast[LocationGlobal]
    global
  }
  def _getHome: Location = {

    val home = Endpoints.primary.PY.home
    val global = home.$MSG.get.cast[LocationGlobal]
    global
  }

  object synch extends SynchronousAPI {

    override def testMove: String = {
      Endpoints.primary.PY.testMove()
        .$STR
        .get
    }

    def clearanceAlt(alt: Double): Unit = {
      Endpoints.primary.PY.assureClearanceAlt(alt) //TODO: should have both maxAlt and minAlt
    }

    override def goto(location: Location): Unit = {
      Endpoints.primary.PY.move(location: PyLocation)
    }
  }
}
