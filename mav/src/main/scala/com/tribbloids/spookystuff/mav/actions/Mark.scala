package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.Export
import com.tribbloids.spookystuff.doc.{Doc, DocUID, Fetched}
import com.tribbloids.spookystuff.session.{NoPythonDriverException, Session}
import org.apache.http.entity.ContentType
import org.apache.spark.ml.dsl.utils.MessageView
import org.json4s.JsonAST.{JField, JObject}

/**
  * Mark current vehicle status
  */
case class Mark() extends Export with MAVAction {

  override def doExeNoName(session: Session): Seq[Fetched] = {

    try {
      val exe = new MAVEXE(session)
      val location = exe.pyLink.vehicle.location
      val global = location.global_frame.$message.get.toJValue
      val globalRelative = location.global_relative_frame.$message.get.toJValue
      val local = location.local_frame.$message.get.toJValue

      val jLocation = JObject(
        JField("Global", global),
        JField("GlobalRelative", globalRelative),
        JField("Local", local)
      )

      val jMark = JObject(
        JField("Location", jLocation)
      )

      Seq(new Doc(
        DocUID((session.backtrace :+ this).toList, this)(),
        exe.link.uri,
        Some(s"${ContentType.APPLICATION_JSON}; charset=UTF-8"),
        MessageView(jMark).prettyJSON.getBytes("UTF8")
      ))
    }
    catch {
      case NoPythonDriverException =>
        Nil
    }
  }
}
