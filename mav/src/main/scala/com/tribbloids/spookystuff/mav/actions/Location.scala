package com.tribbloids.spookystuff.mav.actions

import breeze.linalg.{DenseVector, Vector => Vec}
import com.tribbloids.spookystuff.session.python.CaseInstanceRef
import com.tribbloids.spookystuff.utils.SimpleUDT
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.language.implicitConversions

@SerialVersionUID(-928750192836509428L)
trait Location

//TODO: UDT should not be used extensively, All MAVAction should convert Vectors/Name to Locations on interpolation
class LocationGlobalUDT() extends SimpleUDT[LocationGlobal]
@SQLUserDefinedType(udt = classOf[LocationGlobalUDT])
@SerialVersionUID(56746829410409L)
case class LocationGlobal(
                           lat: Double,
                           lon: Double,
                           alt: Double
                         ) extends Location with CaseInstanceRef


class LocationGlobalRelativeUDT() extends SimpleUDT[LocationGlobalRelative]

/**
  * This is the de-facto standard of specifying location for all outbound commands
  */
@SQLUserDefinedType(udt = classOf[LocationGlobalRelativeUDT])
@SerialVersionUID(-5039218743229730432L)
case class LocationGlobalRelative(
                                   lat: Double,
                                   lon: Double,
                                   alt: Double
                                 ) extends Location with CaseInstanceRef


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

}

object LocationLocal {

  implicit def fromVec(vec: Vec[Double]): LocationLocal = {
    assert(vec.length == 3, "vector is not a 3D coordinate!")
    LocationLocal(vec(0), vec(1), vec(2))
  }
}
