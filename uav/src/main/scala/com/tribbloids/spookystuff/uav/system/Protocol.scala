package com.tribbloids.spookystuff.uav.system

abstract class Protocol extends Serializable

object Protocol {

  case object MAVLink extends Protocol
}
