package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial.point.PointViewBase
import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.ml.dsl.utils.refl.ScalaUDT
import org.apache.spark.sql.types.SQLUserDefinedType

class GeomUDT extends ScalaUDT[Geom[_]]

@SQLUserDefinedType(udt = classOf[GeomUDT])
@SerialVersionUID(-75028740932743L)
trait Geom[+T <: TrellisGeom] extends Spatial with IDMixin {

  def trellisGeom: T
  def jtsGeom: JTSGeom = trellisGeom.jtsGeom
  def vector: DenseVector[Double] = DenseVector(jtsGeom.getCoordinates.flatMap(v => Seq(v.x, v.y, v.z)))

  @transient override lazy val _id = (system.name, trellisGeom)

  lazy val wktString = {
    WKTWriter.write(jtsGeom)
  }

  override lazy val toString = {
    s"${system.name}:$wktString"
    //      + {
    //        this match {
    //          case v: Product =>
    //            ReflectionUtils.getCaseAccessorMap(v).map {
    //              case (vv, d: Double) => s"$vv=${d.formatted("%f")}"
    //              case (vv, d@ _) => s"$vv=$d"
    //            }
    //              .mkString(" ")
    //          case _ => super.toString
    //        }
    //      }
  }

  def toStr_withSearchHistory = (Seq(toString) ++ Option(searchHistory).toSeq).mkString(" ")
}

object Geom {

  implicit class PV(val self: Coordinate) extends PointViewBase

}
