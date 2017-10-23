package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.spatial.util.{SearchAttempt, SearchHistory}
import com.tribbloids.spookystuff.utils.ScalaUDT
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.SQLUserDefinedType
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

//case class UnknownLocation(
//                            id: Long = Random.nextLong()
//                          ) extends LocationLike {
//}

class LocationUDT() extends ScalaUDT[Location]

@SQLUserDefinedType(udt = classOf[LocationUDT])
@SerialVersionUID(-928750192836509428L)
case class Location(
                     definedBy: Seq[Association[Coordinate]],
                     tagOpt: Option[Anchors.Tag] = None
                   ) extends LocationLike with Fusion[Coordinate] {

  override def name = tagOpt.map(_.name).getOrElse("@"+this.hashCode)
  def withHome(home: Location) = WithHome(home)

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
        val replaced: Anchor = fn.applyOrElse(rel.anchor, (_: Anchor) => rel.anchor)
        val taggedReplaced = rel.anchor -> replaced match {
          case (t: Anchors.Tag, v: Location) => v.copy(tagOpt = Some(t))
          case _ => replaced
        }
        rel.copy[Coordinate](
          anchor = taggedReplaced
        )
    }
    this.copy(definedBy = cs)
  }

  private val _mnemonics: ArrayBuffer[Association[Coordinate]] = {
    val result = ArrayBuffer.empty[Association[Coordinate]]
    val preset = definedBy // ++ Seq(Denotation(NED(0,0,0), this))
    result.++=(preset)
    result
  }

  def _cache(tuples: Association[Coordinate]*): this.type = {
    assert(!tuples.contains(null))
    _mnemonics ++= tuples
    //    require(!tuples.exists(_._2 == this), "self referential coordinate cannot be used")
    this
  }
  /**
    * always add result into buffer to avoid repeated computation
    * recursively search through its relations to deduce the coordinate.
    * TODO: this implementation is not designed to infer each dimension individually
    * (e.g. when try to get LLA coord but only has NED to home with altitude known
    * the result can still be (NaN, NaN, Alt) )
    * @param system
    * @param from
    * @param sh
    * @return
    */
  override def _getCoordinate(
                               system: CoordinateSystem,
                               from: Anchor = Anchors.Geodetic,
                               sh: SearchHistory
                             ): Option[system.C] = {

    if (sh.recursions >= 1000) {
      throw new UnsupportedOperationException("too many recursions")
    }

    if (from == this) {
      system.zeroOpt.foreach {
        z =>
          return Some(z)
      }
    }

    def _cacheAndYield(v: system.C): Option[system.C] = {
      _cache(Association[Coordinate](v, from))
      Some(v)
    }

    _mnemonics.foreach {
      rel =>
        if (
          rel.datum.system == system &&
            rel.anchor == from
        ) {
          return Some(rel.datum.asInstanceOf[system.C])
        }
    }

    _mnemonics.foreach {
      rel =>
        val debugStr = s"""
                          |${this.treeString}
                          |-------------
                          |inferring ${system.name} from $from
                          |using ${rel.datum.system.name} from ${rel.anchor}
                          """.trim.stripMargin
        LoggerFactory.getLogger(this.getClass).debug {
          debugStr
        }

        val directOpt: Option[system.C] = rel.datum.project(rel.anchor, from, system, sh)
        directOpt.foreach {
          direct =>
            return _cacheAndYield(direct)
        }

        //use chain rule for inference
        rel.anchor match {
          case middle: Location if middle != this && middle != from =>
            for (
              c2 <- {
                sh.getCoordinate(SearchAttempt(middle, system, this))
              };
              c1 <- {
                sh.getCoordinate(SearchAttempt(from, system, middle))
              }
            ) {
              return _cacheAndYield(c1.asInstanceOf[system.C] :+ c2.asInstanceOf[system.C])
            }
          case _ =>
        }
    }

    //add reverse deduction.

    None
  }

  override def reanchor(anchor: Anchor, system: CoordinateSystem) = {
    val opt = getCoordinate(system, anchor)
    opt.map {
      v =>
        this.copy(
          Seq(v -> anchor)
        ).asInstanceOf[this.type]
    }
  }

  def treeString: String = {
    definedBy.map(_.treeString).mkString("\n")
  }

  lazy val reanchorToPrimary: this.type = {
    val children = definedBy.flatMap {
      v =>
        v.children
    }
    val firstChildOpt = children.collect {
      case Association(x, y: Location) => x -> y
    }
      .headOption
    firstChildOpt.flatMap {
      case (x, y) =>
        this.reanchor(y.reanchorToPrimary, x.system)
    }
      .getOrElse {
        this
      }
      .asInstanceOf[this.type]
  }

  def simpleString: String = {
    name + ": " + reanchorToPrimary
      .definedBy
      .headOption
      .map(_.simpleString)
      .mkString("<", "", ">")
  }

  override def toString: String = simpleString
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

  def parse(v: Any): Location = {
    v match {
      case p: Location =>
        p
      case c: Coordinate =>
        c
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
        c
      case s: String =>
        ???
      case _ =>
        ???
    }
  }
}