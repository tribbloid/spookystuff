package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.DummyUAVFixture
import com.tribbloids.spookystuff.uav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.uav.system.UAV
import org.scalatest.Ignore

/**
  * Created by peng on 6/20/17.
  */
class DummyLinkSuite extends DummyUAVFixture with LinkFixture {

  override lazy val factories: Seq[LinkFactory] = Seq(
    LinkFactories.Dummy
  )
}

//@Ignore
//class DummyLinkSuite_SingleUAV extends DummyLinkSuite {
//
//  override lazy val getFleet: String => Seq[UAV] = {
//    connStr =>
//      Seq(UAV(Seq(connStr)))
//  }
//}
