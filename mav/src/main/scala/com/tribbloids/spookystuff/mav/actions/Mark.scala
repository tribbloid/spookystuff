package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.Export
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Fetched}
import com.tribbloids.spookystuff.session.{NoPythonDriverException, Session}
import org.apache.http.entity.ContentType
import org.apache.spark.ml.dsl.utils.MessageView

case class MarkOutput(
                       location: LocationBundle
                     )

/**
  * Mark current vehicle status
  */
case class Mark() extends Export with MAVAction {

  override def doExeNoName(session: Session): Seq[Fetched] = {

    try {
      val exe = new SessionView(session)
      val location = exe.link.getCurrentLocation
//      val result = MarkOutput(location)
      val jsonStr = MessageView(location).prettyJSON

      Seq(new Doc(
        DocUID((session.backtrace :+ this).toList, this)(),
        exe.link.directEndpoint.uri,
        Some(s"${ContentType.APPLICATION_JSON}; charset=UTF-8"),
        jsonStr.getBytes("UTF8")
      ))
    }
    catch {
      case NoPythonDriverException =>
        Nil
    }
  }
}
