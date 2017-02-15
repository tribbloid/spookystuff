package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.session.python.CaseInstanceRef

import scala.language.implicitConversions

//class LocationUDT() extends ScalaUDT[Location]
//@SQLUserDefinedType(udt = classOf[LocationUDT])
//@SerialVersionUID(-928750192836509428L)
//trait Location extends Serializable {
//
//  def lat: Double
//  def lon: Double
//  def alt: Double
//  def ref: Option[Location] = None
//
//  def _global: LocationGlobal = LocationGlobal(lat, lon, alt)
//
//  def toGlobal(ref: Option[Location] = this.ref): LocationGlobal
//
//  def relativeTo(ref: Location): LocationGlobalRelative = {
//    LocationGlobalRelative(
//      lat,
//      lon,
//      alt - ref.alt,
//      Some(ref)
//    )
//  }
//  def relativeFrom(from: Location): LocationGlobalRelative = from.relativeTo(this)
//
//  //  def localTo(ref: Location): LocationLocal = {
//  //    ???
//  //  }
//
//  //  def localFrom(from: Location): LocationLocal = from.localTo(this)
//
//  /**
//  from http://python.dronekit.io/guide/copter/guided_mode.htmlhttp://python.dronekit.io/guide/copter/guided_mode.html
//
//    Returns a LocationGlobal object containing the latitude/longitude `dNorth` and `dEast` metres from the
//    specified `original_location`. The returned LocationGlobal has the same `alt` value
//    as `original_location`.
//
//    The function is useful when you want to move the vehicle around specifying locations relative to
//    the current vehicle position.
//
//    The algorithm is relatively accurate over small distances (10m within 1km) except close to the poles.
//
//    For more information see:
//    http://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
//    */
//  def _local2LatLon(dNorth: Double, dEast: Double): (Double, Double) = {
//
//    val dLat = dNorth / UAVConf.EARTH_RADIUS
//    val dLon = dEast / (UAVConf.EARTH_RADIUS * Math.cos(Math.PI * this.lat / 180))
//
//    // New position in decimal degrees
//    val newLat = this.lat + (dLat * 180 / Math.PI)
//    val newLon = this.lon + (dLon * 180 / Math.PI)
//    (newLat, newLon)
//  }
//}

trait PyLocation extends CaseInstanceRef

case class LocationGlobal(
                           lat: Double,
                           lon: Double,
                           alt: Double
                         ) extends PyLocation {
}

case class LocationGlobalRelative(
                                   lat: Double,
                                   lon: Double,
                                   altRelative: Double
                                 ) extends PyLocation {
}

case class LocationLocal(
                          north: Double,
                          east: Double,
                          down: Double
                        ) extends PyLocation {
}
