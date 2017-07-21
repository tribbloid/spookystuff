package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.DummyUAVFixture
import com.tribbloids.spookystuff.uav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.uav.system.UAV
import org.scalatest.Ignore

/**
  * Created by peng on 6/20/17.
  */
@Ignore
class DummyLinkSuite extends LinkSuite with DummyUAVFixture {

  override lazy val factories: Seq[LinkFactory] = Seq(
    LinkFactories.Dummy
  )
}

class DummyLinkSuite_SelectFromFleet extends DummyLinkSuite {

  override lazy val getFleet: (String) => Seq[UAV] = {
    val simEndpoints = this.simUAVs
    _: String => simEndpoints
  }
}
