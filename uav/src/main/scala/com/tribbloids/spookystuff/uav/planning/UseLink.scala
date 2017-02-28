package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.UAVAction
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.NOTSerializable

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  */
private[uav] case class UseLink(
                    link: Link
                  ) extends UAVAction with NOTSerializable {

  override def trunk = None

  //  override def exeNoOutput(session: Session): Unit = {
  //    link
  //  }
  override protected def doExe(session: Session): Seq[Fetched] = {
    //    link
    Nil
  }

  override def outputNames: Set[String] = Set.empty
}
