package com.tribbloids.spookystuff.dsl

import java.io.File
import java.util.UUID

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.ml.dsl.utils.MessageView

object FilePaths{

  case object Flat extends ByTrace[String] {

    override def apply(trace: Trace): String = {

      val actionStrs = trace.map(v => MessageView(v).toString_\\\)

      val actionConcat = if (actionStrs.size > 4) {
        val oneTwoThree = actionStrs.slice(0,3)
        val last = actionStrs.last
        val omitted = "..." + (trace.length-4) + "more"+"..."

        oneTwoThree.mkString("~")+omitted+last
      }
      else actionStrs.mkString("~")

      val hash = "-"+trace.hashCode

      SpookyUtils.canonizeFileName(actionConcat + hash)
    }
  }

  case object Hierarchical extends ByTrace[String] {

    override def apply(trace: Trace): String = {

      val actionStrs = trace.map(v => MessageView(v).toString_\\\)

      val actionConcat = if (actionStrs.size > 4) {
        val oneTwoThree = actionStrs.slice(0,3)
        val last = actionStrs.last
        val omitted = File.separator + (trace.length-4) + "more"+ File.separator

        SpookyUtils.\\\(oneTwoThree: _*) + omitted + last
      }
      else SpookyUtils.\\\(actionStrs: _*)

      val hash = "-"+trace.hashCode

      SpookyUtils.canonizeUrn(actionConcat + hash)
    }
  }

  //only from Page
  case class UUIDName(encoder: ByTrace[Any]) extends ByDoc[String] {
    override def apply(page: Doc): String =
      SpookyUtils.\\\(encoder(page.uid.backtrace).toString, UUID.randomUUID().toString)
  }

  case class TimeStampName(encoder: ByTrace[Any]) extends ByDoc[String] {
    override def apply(page: Doc): String =
      SpookyUtils.\\\(encoder(page.uid.backtrace).toString, SpookyUtils.canonizeFileName(page.timeMillis.toString))
  }
}