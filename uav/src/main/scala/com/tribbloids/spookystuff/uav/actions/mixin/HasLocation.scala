package com.tribbloids.spookystuff.uav.actions.mixin

import breeze.linalg.{DenseVector, Vector => Vec}
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UAVAction
import com.tribbloids.spookystuff.uav.spatial.{Location, NED}

/**
  * unless mixin, assume cost is 0
  */
trait HasLocation {
  self: UAVAction =>

  def getLocation(trace: Trace, schema: DataRowSchema): Location

  def getVector(trace: Trace, schema: DataRowSchema): DenseVector[Double] = {
    val location = getLocation(trace, schema)
    val home = schema.ec.spooky.getConf[UAVConf].home
    location.getCoordinate(NED, home)
      .get.vector
  }

  def getStart = getLocation _
  def getEnd = getLocation _
}

trait HasExactLocation extends HasLocation {
  self: UAVAction =>

  override def getLocation(trace: Trace, schema: DataRowSchema): Location = _start

  def _start: Location
}

trait HasLocationDelta extends HasLocation {
  self: UAVAction =>

  def displace(delta: Vec[Double]): this.type

  /**
    * used in SGD-like algorithm to update delta
    * with mllib Updater
    * has built-in 'stiffness' that affects each dimension's tolerance to change
    * @param gradient
    * @param delta
    * @return the new gradient.
    */
  def rewriteGrad(gradient: Vector[Double], delta: Vec[Double]): Vec[Double]
}
