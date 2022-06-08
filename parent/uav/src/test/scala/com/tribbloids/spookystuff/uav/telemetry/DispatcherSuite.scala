package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.DummyUAVFixture
import com.tribbloids.spookystuff.uav.dsl.{Routing, Routings}
import org.scalatest.Ignore

/**
  * Created by peng on 6/20/17.
  * purpose is only testing the concurent execution of Dispatchers
  */
class DispatcherSuite extends DummyUAVFixture with LinkSuite {

  override lazy val routings: Seq[Routing] = Seq(
    Routings.Dummy
  )
}

@Ignore
class DispatcherSuite_Transient extends DispatcherSuite {

  override def onHold = false
}

//@Ignore
//class DummyLinkSuite_SingleUAV extends DummyLinkSuite {
//
//  override lazy val getFleet: String => Seq[UAV] = {
//    connStr =>
//      Seq(UAV(Seq(connStr)))
//  }
//}
