package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.Export
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Fetched}
import com.tribbloids.spookystuff.session.{NoPythonDriverException, Session}
import com.tribbloids.spookystuff.uav.utils.UAVViews.SessionView
import org.apache.http.entity.ContentType
import org.apache.spark.ml.dsl.utils.messaging.MessageView

/**
  * Mark current vehicle status
  */
case class Mark() extends Export with UAVAction {

  override def doExeNoName(session: Session): Seq[Fetched] = {

    try {
      val exe = new SessionView(session)
      val location = exe.link.status().currentLocation
      val jsonStr = MessageView(location).prettyJSON

      Seq(new Doc(
        DocUID((session.backtrace :+ this).toList, this)(),
        exe.link.uav.uris.head,
        Some(s"${ContentType.APPLICATION_JSON}; charset=UTF-8"),
        jsonStr.getBytes("UTF8")
      ))
    }
    catch {
      case e: NoPythonDriverException =>
        Nil
    }
  }
}
