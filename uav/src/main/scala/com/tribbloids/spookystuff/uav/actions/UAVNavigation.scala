package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.Interaction
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.mixin.HasCost
import com.tribbloids.spookystuff.uav.planning.Constraint
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}
import com.tribbloids.spookystuff.uav.utils.UAVViews
import org.apache.spark.ml.uav.Vec

import scala.language.implicitConversions

/**
  * inbound -> engage -> outbound
  */
trait UAVNavigation extends Interaction
  with UAVAction
  with HasCost {

  //TODO: change to Option
  def getLocation(schema: SpookySchema): Location

  //  def vector(trace: Trace, schema: DataRowSchema): DenseVector[Double] = {
  //    val location = getLocation(trace, schema)
  //    val home = schema.ec.spooky.getConf[UAVConf].home
  //    location.getCoordinate(NED, home)
  //      .get.vector
  //  }

  def getStart = getLocation _
  def getEnd = getLocation _

  final val vectorDim = 3

  def shift(vector: Vec): this.type = this

  def constraint: Option[Constraint] = None

  def speedOpt: Option[Double] = None

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.inbound()
    sv.engage()
    sv.outbound()
  }

  def getSessionView(session: Session) = new NavSessionView(session)

  case class WSchema(
                      schema: SpookySchema
                    ) {

    def outer = UAVNavigation.this

    val home = schema.spooky.getConf[UAVConf]._home

    lazy val coordinate = outer.getLocation(schema).coordinate(NED, home)
    lazy val vector = coordinate.vector
  }
}

class NavSessionView(session: Session) extends UAVViews.SessionView(session) {

  def inbound(): Unit = {}

  def engage(): Unit = {}

  def outbound(): Unit = {}
}
