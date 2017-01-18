package com.tribbloids.spookystuff.mav.actions

import breeze.linalg.{DenseVector, Vector => Vec}
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.session.python.CaseInstanceRef
import com.tribbloids.spookystuff.utils.ScalaUDT
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.language.implicitConversions

//TODO: UDT should not be used extensively,
//All MAVAction should convert Vectors/Name to Locations on interpolation
class LocationUDT() extends ScalaUDT[Location]
@SQLUserDefinedType(udt = classOf[LocationUDT])
@SerialVersionUID(-928750192836509428L)
trait Location extends Serializable {

  def lat: Double
  def lon: Double
  def alt: Double
  def ref: Location = LocationGlobal.UnknownLocation

  def _global: LocationGlobal = LocationGlobal(lat, lon, alt)

  def toGlobal(ref: Location = this.ref): LocationGlobal

  def relativeTo(ref: Location): LocationGlobalRelative = {
    LocationGlobalRelative(
      lat,
      lon,
      alt - ref.alt)(
      ref
    )
  }
  def relativeFrom(from: Location): LocationGlobalRelative = from.relativeTo(this)

//  def localTo(ref: Location): LocationLocal = {
//    ???
//  }

//  def localFrom(from: Location): LocationLocal = from.localTo(this)

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
}

@SQLUserDefinedType(udt = classOf[LocationUDT])
@SerialVersionUID(56746829410409L)
case class LocationGlobal(
                           lat: Double,
                           lon: Double,
                           alt: Double
                         ) extends Location with CaseInstanceRef {

  def toGlobal(ref: Location = this.ref) = this
}

object LocationGlobal {

  def Altitude(alt: Double) = LocationGlobal(Double.NaN, Double.NaN, alt)
  val UnknownLocation = LocationGlobal(Double.NaN, Double.NaN, Double.NaN)
}

@SQLUserDefinedType(udt = classOf[LocationUDT])
@SerialVersionUID(-5039218743229730432L)
case class LocationGlobalRelative(
                                   lat: Double,
                                   lon: Double,
                                   altRelative: Double)(
                                   override val ref: Location = LocationGlobal.UnknownLocation // cannot be omitted
                                 ) extends Location {

  val alt = altRelative + ref.alt

  def toGlobal(ref: Location = this.ref) = this.copy()(ref = ref)._global
}

@SQLUserDefinedType(udt = classOf[LocationUDT])
@SerialVersionUID(4604257236921846832L)
case class LocationLocal(
                          north: Double,
                          east: Double,
                          down: Double)(
                          override val ref: Location = LocationGlobal.UnknownLocation // cannot be omitted
                        ) extends Location {

  def vec: DenseVector[Double] = DenseVector[Double](Array(north, east, down))

  override val alt: Double = ref.alt - down

  override val (lat, lon) = ref._local2LatLon(north, east)

  def toGlobal(ref: Location = this.ref) = this.copy()(ref = ref)._global
}

object LocationLocal {

  implicit def fromVec(vec: Vec[Double]): LocationLocal = {
    assert(vec.length == 3, "vector is not 3D!")
    LocationLocal(vec(0), vec(1), vec(2))()
  }
}
