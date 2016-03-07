package com.tribbloids.spookystuff.dsl

import java.util.UUID

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.pages.Page
import com.tribbloids.spookystuff.utils.Utils

/**
 * Created by peng on 9/12/14.
 */

object FilePaths{

  case object Flat extends ByTrace[String] {

    override def apply(trace: Trace): String = {

      val actionStrs = trace.map(_.toString)

      val actionConcat = if (actionStrs.size > 4) {
        val oneTwoThree = actionStrs.slice(0,3)
        val last = actionStrs.last
        val omitted = "..."+(trace.length-4).toString+"more"+"..."

        oneTwoThree.mkString("~")+omitted+last
      }
      else actionStrs.mkString("~")

      val hash = "-"+trace.hashCode

      Utils.canonizeFileName(actionConcat + hash)
    }
  }

  case object Hierarchical extends ByTrace[String] {

    override def apply(trace: Trace): String = {

      val actionStrs = trace.map(_.toString)

      val actionConcat = if (actionStrs.size > 4) {
        val oneTwoThree = actionStrs.slice(0,3)
        val last = actionStrs.last
        val omitted = "/"+(trace.length-4).toString+"more"+"/"

        oneTwoThree.mkString("/")+omitted+last
      }
      else actionStrs.mkString("/")

      val hash = "-"+trace.hashCode

      Utils.canonizeUrn(actionConcat + hash)
    }
  }

  //only from Page
  case class UUIDName(encoder: ByTrace[Any]) extends ByPage[String] {
    override def apply(page: Page): String =
      Utils.uriConcat(encoder(page.uid.backtrace).toString, UUID.randomUUID().toString)
  }

  case class TimeStampName(encoder: ByTrace[Any]) extends ByPage[String] {
    override def apply(page: Page): String =
      Utils.uriConcat(encoder(page.uid.backtrace).toString, Utils.canonizeFileName(page.timestamp.toString))
  }
}