package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.DummyUAVFixture
import com.tribbloids.spookystuff.uav.dsl.{Routing, Routings}

/**
  * Created by peng on 6/20/17.
  * purpose is only testing the concurent execution of Dispatchers
  */
class DispatcherSuite extends DummyUAVFixture with LinkSuite {

  override lazy val factories: Seq[Routing] = Seq(
    Routings.Dummy
  )
}

//class DispatcherSuite_OnHold extends DispatcherSuite {
//
//  override def onHold = true
//}

//@Ignore
//class DummyLinkSuite_SingleUAV extends DummyLinkSuite {
//
//  override lazy val getFleet: String => Seq[UAV] = {
//    connStr =>
//      Seq(UAV(Seq(connStr)))
//  }
//}
