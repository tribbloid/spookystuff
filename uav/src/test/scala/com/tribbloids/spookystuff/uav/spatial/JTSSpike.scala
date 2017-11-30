package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.testutils.FunSpecx
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import com.vividsolutions.jts.io.WKTReader

class JTSSpike extends FunSpecx {

  val geoFactory = new GeometryFactory()

  describe("WKTReader") {
    it("Can parse 3D point") {
      var parser = new WKTReader(geoFactory)

      val obj = parser.read("POINT (30 10 20)")
      val expectedCoord = new JTSCoord(30, 10, 20)
      assert(obj.getClass == classOf[Point])
      assert(obj == geoFactory.createPoint(expectedCoord))
    }
  }
}
