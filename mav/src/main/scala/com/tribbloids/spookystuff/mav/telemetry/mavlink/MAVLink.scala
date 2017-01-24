package com.tribbloids.spookystuff.mav.telemetry.mavlink

import com.tribbloids.spookystuff.mav.actions.LocationGlobal
import com.tribbloids.spookystuff.mav.system.Drone
import com.tribbloids.spookystuff.mav.telemetry.Link
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.session.{Cleanable, ResourceLedger}
import org.slf4j.LoggerFactory

object MAVLink {

  def sanityCheck(): Unit = {
    val subs = Cleanable.getTyped[Endpoint] ++ Cleanable.getTyped[Proxy]
    val refSubs = Cleanable.getTyped[MAVLink].flatMap(_.subCleanable)
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
                    drone: Drone,
                    executorOuts: Seq[String] = Nil, // cannot have duplicates
                    gcsOuts: Seq[String] = Nil
                  ) extends Link with ResourceLedger {

  {
    if (executorOuts.isEmpty) assert(gcsOuts.isEmpty, "No endpoint for executor")
  }

  override lazy val resourceIDs = Map("" -> (drone.uris ++ executorOuts).toSet)

  val outs: Seq[String] = executorOuts ++ gcsOuts
  val allURIs = (drone.uris ++ outs).distinct

  /**
    * CAUTION: ALL of them have to be val or lazy val! Or you risk recreating many copies each with its own python!
    * Conflict with each other!
    */
  object Endpoints {
    val direct: Endpoint = {
      Endpoint(
        drone.uris.head,
        drone.baudRate,
        drone.endpointSSID,
        drone.frame
      )
    }
    val executors: Seq[Endpoint] = if (executorOuts.isEmpty) {
      Seq(direct)
    }
    else {
      executorOuts.map {
        out =>
          direct.copy(uri = out)
      }
    }
    //always initialized in Python when created from companion object
    val primary: Endpoint = executors.head
    val GCSs: Seq[Endpoint] = {
      gcsOuts.map {
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
        name = drone.name
      )
      Some(proxy)
    }
    result
  }

  def coFactory(another: Link): Boolean = {
    another match {
      case v: MAVLink =>
        val result = this.gcsOuts.toSet == v.gcsOuts.toSet
        if (!result) {
          LoggerFactory.getLogger(this.getClass).info (
            s"""
               |Cannot use existing MAVLink for ${v.drone}:
               |output should be routed to GCS(s) ${v.gcsOuts.mkString("[",", ","]")}
               |but instead existing one routes it to ${gcsOuts.mkString("[",", ","]")}
             """.trim.stripMargin
          )
        }
        result
      case _ =>
        false
    }
  }

  override protected def detectConflicts(): Unit = {
    def uri = Endpoints.primary.uri
    val drivers = Cleanable.getTyped[PythonDriver]
    val conflicting = drivers.filter {
      driver =>
        driver.historyCodeOpt.exists(_.contains(uri))
    }
    val notMe = conflicting.filter {
      driver =>
        !(driver eq Endpoints.primary.PY.driver)
    }
    assert(notMe.isEmpty,
      s"Besides legitimate processs PID=${Endpoints.primary.PY.driver.getPid}. The following python process(es) also use $uri\n" +
        notMe.map {
          driver =>
            s"=============== PID=${driver.getPid} ===============\n" +
              driver.historyCodeOpt.getOrElse("")
        }
          .mkString("\n")
    )
  }

  override def subCleanable: Seq[Cleanable] = {
    Endpoints.all ++
      proxyOpt.toSeq ++
      super.subCleanable
  }

  protected def _connect(): Unit = {
    proxyOpt.foreach {
      _.PY.start()
    }
    Endpoints.primary.PY.start()
  }
  protected def _disconnect(): Unit = {
    Endpoints.primary.PY.stop()
    proxyOpt.foreach {
      _.PY.stop()
    }
  }

  def _getLocation: LocationGlobal = {

    val locations = Endpoints.primary.PY.vehicle.location
    val global = locations.global_frame.$MSG.get.cast[LocationGlobal]
    global
  }
  def _getHome: LocationGlobal = {

    val home = Endpoints.primary.PY.home
    val global = home.$MSG.get.cast[LocationGlobal]
    global
  }

  object Synch extends SynchronousAPI {

    override def testMove: String = {
      Endpoints.primary.PY.testMove()
        .$STR
        .get
    }

    def clearanceAlt(alt: Double): Unit = {
      Endpoints.primary.PY.assureClearanceAlt(alt)
    }

    override def move(location: LocationGlobal): Unit = {
      Endpoints.primary.PY.move(location)
    }
  }
}