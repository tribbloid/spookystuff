package com.tribbloids.spookystuff.mav.actions

import breeze.linalg.{DenseVector, Vector => Vec}
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.session.python.CaseInstanceRef
import com.tribbloids.spookystuff.utils.SimpleUDT
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.language.implicitConversions

@SerialVersionUID(-928750192836509428L)
trait Location {
  def alt: Double
}

trait AbstractLocationGlobal extends Location {

  def lat: Double
  def lon: Double

  def absolute: LocationGlobal

  def relativeTo(ref: AbstractLocationGlobal): LocationGlobalRelative = {
    val abs = absolute
    LocationGlobalRelative(
      lat,
      lon,
      abs.alt - ref.absolute.alt
    )(
      ref
    )
  }

  def relativeFrom(from: AbstractLocationGlobal): LocationGlobalRelative = from.relativeTo(this)

  /**
  from http://python.dronekit.io/guide/copter/guided_mode.htmlhttp://python.dronekit.io/guide/copter/guided_mode.html

    Returns a LocationGlobal object containing the latitude/longitude `dNorth` and `dEast` metres from the
    specified `original_location`. The returned LocationGlobal has the same `alt` value
    as `original_location`.

    The function is useful when you want to move the vehicle around specifying locations relative to
    the current vehicle position.

    The algorithm is relatively accurate over small distances (10m within 1km) except close to the poles.

    For more information see:
    http://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
    */
  def _local2LatLon(dNorth: Double, dEast: Double): (Double, Double) = {

    val dLat = dNorth / MAVConf.EARTH_RADIUS
    val dLon = dEast / (MAVConf.EARTH_RADIUS * Math.cos(Math.PI * this.lat / 180))

    // New position in decimal degrees
    val newLat = this.lat + (dLat * 180 / Math.PI)
    val newLon = this.lon + (dLon * 180 / Math.PI)
    (newLat, newLon)
  }

  def relativeFromLocal(local: LocationLocal): LocationGlobalRelative = {
    val latlon = _local2LatLon(local.north, local.east)
    LocationGlobalRelative(
      latlon._1,
      latlon._2,
      local.alt
    )(
      this
    )
  }
}

//TODO: UDT should not be used extensively, All MAVAction should convert Vectors/Name to Locations on interpolation
class LocationGlobalUDT() extends SimpleUDT[LocationGlobal]
@SQLUserDefinedType(udt = classOf[LocationGlobalUDT])
@SerialVersionUID(56746829410409L)
case class LocationGlobal(
                           lat: Double,
                           lon: Double,
                           alt: Double
                         ) extends AbstractLocationGlobal with CaseInstanceRef {

  override lazy val absolute: LocationGlobal = this
}

class AltGlobal(alt: Double) extends LocationGlobal(Double.NaN, Double.NaN, alt)

class LocationGlobalRelativeUDT() extends SimpleUDT[LocationGlobalRelative] {}

/**
  * This is the de-facto standard of specifying location for all outbound commands
  */
//TODO: native to Dronekit if based on home location, should convert all to this before being sent out
//otherwise the local reference of each drone will be different and causes massive confusion
@SQLUserDefinedType(udt = classOf[LocationGlobalRelativeUDT])
@SerialVersionUID(-5039218743229730432L)
case class LocationGlobalRelative(
                                   lat: Double,
                                   lon: Double,
                                   alt: Double
                                 )(
                                   base: AbstractLocationGlobal // cannot be omitted
                                 ) extends AbstractLocationGlobal with CaseInstanceRef {

  override lazy val absolute: LocationGlobal = LocationGlobal(
    lat, lon, alt + base.absolute.alt
  )
}

class LocationLocalUDT() extends SimpleUDT[LocationLocal]

/**
  * always use NED coordinate
  */
@SQLUserDefinedType(udt = classOf[LocationLocalUDT])
@SerialVersionUID(4604257236921846832L)
case class LocationLocal(
                          north: Double,
                          east: Double,
                          down: Double
                        ) extends DenseVector[Double](Array(north, east, down)) with Location with CaseInstanceRef {

  def relativeTo(ref: AbstractLocationGlobal): LocationGlobalRelative = ref.relativeFromLocal(this)

  override def alt: Double = -down
}

object LocationLocal {

  implicit def fromVec(vec: Vec[Double]): LocationLocal = {
    assert(vec.length == 3, "vector is not a 3D coordinate!")
    LocationLocal(vec(0), vec(1), vec(2))
  }
}
