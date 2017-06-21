package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.uav.system.UAV

/**
  * Created by peng on 6/20/17.
  */
class DummyLinkSuite extends LinkFixture {

  override lazy val getFleet: (String) => Seq[UAV] = {
    val simEndpoints = this.simUAVs
    _: String => simEndpoints
  }

  override def simURIs = (0 until parallelism).map {
    v =>
      s"dummy:localhost:$v"
  }

  override lazy val factories: Seq[LinkFactory] = Seq(
    LinkFactories.Dummy
  )
}
