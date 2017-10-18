package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial.util.{SearchAttempt, SearchHistory}
import com.tribbloids.spookystuff.uav.spatial.{Anchor, Spatial}
import com.tribbloids.spookystuff.utils.ReflectionUtils
import org.apache.spark.mllib.uav.Vec
import org.osgeo.proj4j.ProjCoordinate

import scala.language.implicitConversions

trait Coordinate extends Spatial {

  def x: Double = vector(0)
  def y: Double = vector(1)
  def z: Double = vector(2)

  //implement this to bypass proj4
  def fastProjectTo(
                     ref1: Anchor,
                     ref2: Anchor, system2: CoordinateSystem,
                     ic: SearchHistory
                   ): Option[system2.C] = {
    system2 match {
      case NED if ref1 == ref2 => Some(NED.C(0,0,0).asInstanceOf[system2.C])
      case _ => None
    }
  }

  def projectZ(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, ic: SearchHistory): Option[Double] = {

    val delta2_1Opt = {
      ic.getCoordinate(SearchAttempt(ref2, LLA, ref1))
        .map(_.alt)
        .orElse {
          ic.getCoordinate(SearchAttempt(ref1, LLA, ref2))
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
             ): Option[system2.C] = {

    val customResult: Option[system2.C] = fastProjectTo(ref1, ref2, system2, ic)
      .map(_.asInstanceOf[system2.C]) // redundant! IDE error?
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
        system2.create(vec, ic)
      }
    }
  }

  override lazy val toString = {
    s"${this.system.name} " + {
      this match {
        case v: Product =>
          ReflectionUtils.getCaseAccessorMap(v).map {
            case (vv, d: Double) => s"$vv=${d.formatted("%f")}"
            case (vv, d@ _) => s"$vv=$d"
          }
            .mkString(" ")
        case _ => super.toString
      }
    }
  }

  def toStr_withSearchHistory = (Seq(toString) ++ Option(searchHistory).toSeq).mkString(" ")
}
