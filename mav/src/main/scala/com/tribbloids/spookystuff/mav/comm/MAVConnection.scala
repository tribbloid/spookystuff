package com.tribbloids.spookystuff.mav.comm

import com.tribbloids.spookystuff.session.python.{CaseInstanceRef, StaticRef}

object MAVConnection extends StaticRef

/**
  * Created by peng on 29/10/16.
  * problems so far:

to keep a drone in the air, a python daemon process D has to be constantly running to
supervise task-irrelevant path planning (e.g. RTL/Altitude Hold/Avoidance).
This process outlives the driver process. Who launches D? how to ensure smooth transitioning
of control during Partition1 => D => Partition2 ? Can they share the same
Connection / Endpoint / Proxy ? Do you have to make them picklable ?

GCS:UDP:xxx ------------------------> Proxy:TCP:xxx -> Drone
                                   /
TaskProcess -> Connection:UDP:xx -/
            /
DaemonProcess   (can this be delayed to be implemented later? completely surrender control to GCS after Altitude Hold)
  is Vehicle picklable? if yes then that changes a lot of things.
  but if not ...
    how to ensure that an interpreter can takeover and get the same vehicle?
  */
case class MAVConnection(
                          endpoint: Endpoint,
                          proxy: Option[Proxy]
                        ) extends CaseInstanceRef