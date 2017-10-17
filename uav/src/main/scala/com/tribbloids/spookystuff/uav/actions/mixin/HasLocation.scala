package com.tribbloids.spookystuff.uav.actions.mixin

import com.tribbloids.spookystuff.actions.{RewriteRule, Trace}
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.actions.UAVAction
import com.tribbloids.spookystuff.uav.spatial.Location
import org.apache.spark.mllib.uav.Vec

/**
  * unless mixin, assume cost is 0
  *
  * used in SGD-like algorithm to update delta
  * with mllib Updater
  * has built-in 'stiffness' that affects each dimension's tolerance to change
  */
trait HasLocation extends RewriteRule[Vec] {
  self: UAVAction =>

  def getLocation(trace: Trace, schema: DataRowSchema): Location = ???

//  def vector(trace: Trace, schema: DataRowSchema): DenseVector[Double] = {
//    val location = getLocation(trace, schema)
//    val home = schema.ec.spooky.getConf[UAVConf].home
//    location.getCoordinate(NED, home)
//      .get.vector
//  }

  def getStart = getLocation _
  def getEnd = getLocation _

  final val vectorDim = 3

  def shiftLocation(vector: Array[Double]): this.type = this
}

trait HasExactLocation extends HasLocation {
  self: UAVAction =>

  override def getLocation(trace: Trace, schema: DataRowSchema): Location = _start

  def _start: Location
}