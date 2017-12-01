package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.testutils.FunSpecx
import com.vividsolutions.jts.geom.{GeometryFactory, Point}

class JTSSpike extends FunSpecx {

  val geoFactory = new GeometryFactory()

  describe("WKTReader") {
    it("Can parse 3D point") {

      val geom = WKTReader.read("POINT (30 10 20)")
      val expectedCoord = new JTSCoord(30, 10, 20)
      assert(geom.getClass == classOf[Point])
      assert(geom == geoFactory.createPoint(expectedCoord))

      val str = WKTWriter.write(geom)
      str.shouldBe(
        "POINT (30 10 20)"
      )
    }
  }
}
