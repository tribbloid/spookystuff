package com.tribbloids.spookystuff.uav.spatial

/**
  * Created by peng on 15/02/17.
  */

trait StartEndLocation {

  def start: LocationLike
  def end: LocationLike

  def speed: Double = Double.MaxValue
}
