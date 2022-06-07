package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.locationtech.jts.geom.{GeometryFactory, Point}

class WKTReadWriteSuite extends FunSpecx {

  val geoFactory = new GeometryFactory()

  it("Can parse and write 3D point") {

    val pointStr = "POINT (30 10 20)"

    val geom = WKTReader.read(pointStr)
    val expectedCoord = new JTSCoord(30, 10, 20)
    assert(geom.getClass == classOf[Point])
    assert(geom == geoFactory.createPoint(expectedCoord))

    val str = WKTWriter.write(geom)
    str.shouldBe(
      pointStr
    )
  }
}
