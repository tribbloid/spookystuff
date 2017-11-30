package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial.point.{CoordinateSystem, LLA, PointViewBase}
import com.tribbloids.spookystuff.uav.spatial.util.{SearchAttempt, SearchHistory}
import org.apache.spark.mllib.uav.Vec
import org.osgeo.proj4j.ProjCoordinate

trait Spatial extends Serializable {

  def system: CoordinateSystem
  def vector: DenseVector[Double]


  var searchHistory: SearchHistory = _
}

trait GeomSpatial[+T <: TrellisGeom] extends Spatial {

  def geom: T
  def jtsGeom: JTSGeom = geom.jtsGeom
  def vector: DenseVector[Double] = DenseVector(jtsGeom.getCoordinates.flatMap(v => Seq(v.x, v.y, v.z)))
}
//
object Spatial {

  implicit class PointView[C <: Coordinate](override val self: C) extends PointViewBase(self) {

    def projectZ(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, ic: SearchHistory): Option[Double] = {

      val attempt1 = SearchAttempt[LLA.type ](ref2, LLA, ref1)
      val attempt2 = SearchAttempt[LLA.type ](ref1, LLA, ref2)

      val delta2_1Opt: Option[Double] = {
        ic.getCoordinate(attempt1)
          .map(_.alt)
          .orElse {
            ic.getCoordinate(attempt2)
              .map(v => - v.alt)
          }
      }

      delta2_1Opt.map {
        delta =>
          delta + z
      }
    }

    /**
      * @param ref2 origin of the new coordinate system
      * @param system2 type of the new coordinate system
      * @return new coordinate
      */
    def project(
                 ref1: Anchor,
                 ref2: Anchor,
                 system2: CoordinateSystem,
                 ic: SearchHistory
               ): Option[system2.Coordinate] = {

      val customResult: Option[system2.Coordinate] = fastProject(ref1, ref2, system2, ic)
//        .map(_.asInstanceOf[system2.Coordinate]) // redundant! IDE error?
      customResult.orElse {
        val dstZOpt = projectZ(ref1, ref2, system2, ic)
        for (
          proj1 <- system.get2DProj(ref1, ic);
          proj2 <- system2.get2DProj(ref2, ic);
          dstZ <- dstZOpt
        ) yield {
          val src = new ProjCoordinate(x, y)
          val wgs84 = new ProjCoordinate
          val dst = new ProjCoordinate
          proj1.inverseProject(src, wgs84)
          proj2.project(wgs84, dst)
          val vec: Vec = Vec(dst.x, dst.y, dstZ)
          system2.fromVec(vec, ic)
        }
      }
    }
  }
}
