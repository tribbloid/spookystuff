package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.UAVAction
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.NOTSerializable

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will trySelect another.
  */
private[uav] case class PreferLink(
                                    links: Link*
                                  ) extends UAVAction with NOTSerializable {

  val firstLink = links.head

  override def trunk = None

  override protected def doExe(session: Session): Seq[Fetched] = {
    Nil
  }

  override def outputNames: Set[String] = Set.empty
}
