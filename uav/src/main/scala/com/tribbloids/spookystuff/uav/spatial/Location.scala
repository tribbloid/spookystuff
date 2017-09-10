package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.utils.ScalaUDT
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.SQLUserDefinedType
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

trait LocationLike extends Anchor {
}

//case class UnknownLocation(
//                            id: Long = Random.nextLong()
//                          ) extends LocationLike {
//}

class LocationUDT() extends ScalaUDT[Location]

// to be enriched by SpookyContext (e.g. baseLocation, CRS etc.)
@SQLUserDefinedType(udt = classOf[LocationUDT])
@SerialVersionUID(-928750192836509428L)
case class Location(
                     definedBy: Seq[SpatialRelation]
                   ) extends LocationLike {


  case class WithHome(
                       home: Location
                     ) {

    lazy val homeLevelProj: Location = {
      val ned = Location.this.getCoordinate(NED, home).get
      Location.fromTuple(ned.copy(down = 0) -> home)
    }

    lazy val MSLProj: Location = {
      val lle = Location.this.getCoordinate(LLA, home).get
      Location.fromTuple(lle.copy(alt = 0) -> Anchors.Geodetic)
    }
  }

  /**
    * replace all PlaceHoldingAnchor with ref.
    */
  def replaceAnchors(fn: PartialFunction[Anchor, Anchor]): Location = {

    val cs = definedBy.map {
      rel =>
        //TODO: unnecessary copy if out of fn domain
        val replaced: Anchor = fn.applyOrElse(rel.from, _ => rel.from)
        rel.copy(
          from = replaced
        )
    }
    this.copy(definedBy = cs)
  }

  private val mnemonics: ArrayBuffer[SpatialRelation] = {
    val result = ArrayBuffer.empty[SpatialRelation]
    val preset = definedBy // ++ Seq(Denotation(NED(0,0,0), this))
    result.++=(preset)
    result
  }

  def addCoordinate(tuples: SpatialRelation*): this.type = {
    assert(!tuples.contains(null))
    mnemonics ++= tuples
    //    require(!tuples.exists(_._2 == this), "self referential coordinate cannot be used")
    this
  }

  // always add result into buffer to avoid repeated computation
  // recursively search through its relations to deduce the coordinate.
  override def _getCoordinate(
                               system: CoordinateSystem,
                               from: Anchor = Anchors.Geodetic,
                               ic: SearchHistory
                             ): Option[system.V] = {

    if (ic.recursions >= 1000) {
      throw new UnsupportedOperationException("too many recursions")
    }

    if (from == this) {
      system.zero.foreach {
        z =>
          return Some(z)
      }
    }

    val allRelations = mnemonics

    def cacheAndYield(v: Coordinate): Option[system.V] = {
      addCoordinate(SpatialRelation(v, from))
      Some(v.asInstanceOf[system.V])
    }

    allRelations.foreach {
      rel =>
        if (
          rel.coordinate.system == system &&
            rel.from == from
        ) {
          return Some(rel.coordinate.asInstanceOf[system.V])
        }
    }

    allRelations.foreach {
      rel =>
        val debugStr = s"""
                          |${this.treeString}
                          |-------------
                          |inferring ${system.name} from $from
                          |using ${rel.coordinate.system.name} from ${rel.from}
                          """.trim.stripMargin
        LoggerFactory.getLogger(this.getClass).debug {
          debugStr
        }

        val directOpt: Option[CoordinateSystem#V] = rel.project(from, system, ic)
        directOpt.foreach {
          direct =>
            return cacheAndYield(direct)
        }

        //use chain rule for inference
        rel.from match {
          case middle: Location if middle != this && middle != from =>
            for (
              c2 <- {
                ic.getCoordinate(SpatialEdge(middle, system, this))
              };
              c1 <- {
                ic.getCoordinate(SpatialEdge(from, system, middle))
              }
            ) {
              return cacheAndYield(c1.asInstanceOf[system.V] ++> c2.asInstanceOf[system.V])
            }
          case _ =>
        }
    }

    //add reverse deduction.

    None
  }

  def treeString: String = {
    definedBy.map(_.treeString).mkString("\n")
  }

  override def toString: String = {
    definedBy.headOption.map {
      _.coordinate
    }
      .mkString("<", "", ">")
  }
}

object Location {

  implicit def fromCoordinate(
                               c: Coordinate
                             ): Location = {
    Location(Seq(c -> Anchors.Home))
  }

  implicit def fromTuple(
                          t: (Coordinate, Anchor)
                        ): Location = {
    Location(Seq(t))
  }

  def parse(v: Any, conf: UAVConf): Location = {
    v match {
      case p: Location =>
        p.replaceAnchors(conf.home)
      case c: Coordinate =>
        c.replaceAnchors(conf.home)
      case v: GenericRowWithSchema =>
        val schema = v.schema
        val names = schema.fields.map(_.name)
        val c = if (names.containsSlice(Seq("lat", "lon", "alt"))) {
          LLA(
            v.getAs[Double]("lat"),
            v.getAs[Double]("lon"),
            v.getAs[Double]("alt")
          )
        }
        else if (names.containsSlice(Seq("north", "east", "down"))) {
          NED(
            v.getAs[Double]("north"),
            v.getAs[Double]("east"),
            v.getAs[Double]("down")
          )
        }
        else {
          ???
        }
        c.replaceAnchors(conf.home)
      case s: String =>
        ???
      case _ =>
        ???
    }
  }

  //TODO: from Row

  //  def d[T <: CoordinateLike : ClassTag](a: Position, b: Position): Try[Double] = {
  //    val ctg = implicitly[ClassTag[T]]
  //    dMat((a, b, ctg))
  //  }
}