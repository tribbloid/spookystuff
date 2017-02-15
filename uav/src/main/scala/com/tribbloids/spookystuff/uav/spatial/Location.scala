package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.caching.ConcurrentSet
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.utils.{IDMixin, ScalaUDT}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.SQLUserDefinedType
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.util.Random

/**
  * Created by peng on 15/02/17.
  */
trait Anchor extends Serializable {

  def getCoordinate(system: CoordinateSystem = LLA, from: Anchor = GeodeticAnchor): Option[system.V] = {
    _getCoordinate(system, from, Set.empty).map(_.asInstanceOf[system.V])
  }

  def coordinate(system: CoordinateSystem = LLA, from: Anchor = GeodeticAnchor): system.V = {
    getCoordinate(system, from).getOrElse {
      throw new UnsupportedOperationException(s"cannot determine relative position from $from to $this")
    }
      .asInstanceOf[system.V]
  }

  def _getCoordinate(system: CoordinateSystem, from: Anchor = GeodeticAnchor, cyclic: Set[Anchor]): Option[system.V]
}

case object GeodeticAnchor extends Anchor {
  override def _getCoordinate(system: CoordinateSystem, from: Anchor = GeodeticAnchor, cyclic: Set[Anchor]): Option[system.V] = {
    None
  }
}
// to be injected by SpookyConf
case object PlaceHoldingAnchor extends Anchor {
  override def _getCoordinate(system: CoordinateSystem, from: Anchor = GeodeticAnchor, cyclic: Set[Anchor]): Option[system.V] = None
}

class LocationUDT() extends ScalaUDT[Location]

// to be enriched by SpookyContext (e.g. baseLocation, CRS etc.)
@SQLUserDefinedType(udt = classOf[LocationUDT])
@SerialVersionUID(-928750192836509428L)
case class Location(
                     coordinates: Seq[Denotation] = Nil,
                     _id: Long = Random.nextLong()
                   ) extends Anchor with IDMixin with StartEndLocation {

  //  require(!coordinates.exists(_._2 == this), "self referential coordinate cannot be used")

  def assumeAnchor(ref: Anchor): Location = {
    val cs = coordinates.map {
      tuple =>
        if (tuple.anchor == PlaceHoldingAnchor) {
          require(coordinates.size == 1, "")
          tuple.copy(anchor = ref)
        }
        else tuple
    }
    this.copy(coordinates = cs)
  }

  val cache: ConcurrentSet[Denotation] = {
    val result = ConcurrentSet[Denotation]()
    val preset = coordinates // ++ Seq(Denotation(NED(0,0,0), this))
    result.++=(preset)
    result
  }

  def addCoordinate(tuples: Denotation*): this.type = {
    assert(!tuples.contains(null))
    cache ++= tuples
    //    require(!tuples.exists(_._2 == this), "self referential coordinate cannot be used")
    this
  }

  // always add result into buffer to avoid repeated computation
  // recursively search through its relations to deduce the coordinate.
  def _getCoordinate(system: CoordinateSystem, from: Anchor = GeodeticAnchor, cyclic: Set[Anchor]): Option[system.V] = {

    LoggerFactory.getLogger(this.getClass).info(
      s"getting $system coordinate from $from"
    )

    if (from == this && system == NED) return Some(NED.V(0,0,0).asInstanceOf[system.V])
    val allCoordinates: ConcurrentSet[Denotation] = cache.filter {
      v =>
        !cyclic.contains(v.anchor)
    }

    val exactMatch = allCoordinates.find(v => v.coordinate.system == system && v.anchor == from)
    exactMatch.foreach {
      tuple =>
        return Some(tuple.coordinate.asInstanceOf[system.V])
    }

    def cacheAndBox(v: Coordinate): Option[system.V] = {
      addCoordinate(Denotation(v, from))
      Some(v.asInstanceOf[system.V])
    }

    val nextCyclic = cyclic + this
    allCoordinates
      .foreach {
        tuple =>
          val directOpt: Option[CoordinateSystem#V] = tuple.projectTo(from, system, nextCyclic)
          directOpt.foreach {
            direct =>
              return cacheAndBox(direct)
          }

          //use chain rule for inference
          tuple.anchor match {
            case p: Location if p != this && p != from =>
              val c1Opt: Option[system.V] = p._getCoordinate(system, from, nextCyclic)
                .map(_.asInstanceOf[system.V])
              val c2Opt: Option[system.V] = this._getCoordinate(system, p, nextCyclic)
                .map(_.asInstanceOf[system.V])
              for (
                c1 <- c1Opt;
                c2 <- c2Opt
              ) {
                return cacheAndBox(c1 ++> c2)
              }
            case _ =>
          }
      }

    //add reverse deduction.

    None
  }

  override def start: Location = this

  override def end: Location = this
}

object Location {

  implicit def fromCoordinate(
                               c: Coordinate
                             ): Location = {
    Location(Seq(
      Denotation(c, PlaceHoldingAnchor)
    ))
  }

  implicit def fromTuple(
                          t: (Coordinate, Anchor)
                        ): Location = {
    Location(Seq(
      Denotation(t._1, t._2)
    ))
  }

  def parse(v: Any, conf: UAVConf): Location = {
    v match {
      case p: Location =>
        p.assumeAnchor(conf.homeLocation)
      case c: Coordinate =>
        c.assumeAnchor(conf.homeLocation)
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
        c.assumeAnchor(conf.homeLocation)
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