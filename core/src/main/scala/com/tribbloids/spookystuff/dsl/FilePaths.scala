package com.tribbloids.spookystuff.dsl

import java.util.UUID

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.utils.Utils

object FilePaths{

  def product2String(x: Product): String = {
    x.productIterator
        .map {
          case vv: Product => product2String(vv)
          case vv@ _ => "" + vv
        }

      .mkString(x.productPrefix + "/", "/", "/")
  }

  case object Flat extends ByTrace[String] {

    override def apply(trace: Trace): String = {

      val actionStrs = trace.map(product2String)

      val actionConcat = if (actionStrs.size > 4) {
        val oneTwoThree = actionStrs.slice(0,3)
        val last = actionStrs.last
        val omitted = "..." + (trace.length-4) + "more"+"..."

        oneTwoThree.mkString("~")+omitted+last
      }
      else actionStrs.mkString("~")

      val hash = "-"+trace.hashCode

      Utils.canonizeFileName(actionConcat + hash)
    }
  }

  case object Hierarchical extends ByTrace[String] {

    override def apply(trace: Trace): String = {

      val actionStrs = trace.map(product2String)

      val actionConcat = if (actionStrs.size > 4) {
        val oneTwoThree = actionStrs.slice(0,3)
        val last = actionStrs.last
        val omitted = "/" + (trace.length-4) + "more"+"/"

        oneTwoThree.mkString("/")+omitted+last
      }
      else actionStrs.mkString("/")

      val hash = "-"+trace.hashCode

      Utils.canonizeUrn(actionConcat + hash)
    }
  }

  //only from Page
  case class UUIDName(encoder: ByTrace[Any]) extends ByDoc[String] {
    override def apply(page: Doc): String =
      Utils.pathConcat(encoder(page.uid.backtrace).toString, UUID.randomUUID().toString)
  }

  case class TimeStampName(encoder: ByTrace[Any]) extends ByDoc[String] {
    override def apply(page: Doc): String =
      Utils.pathConcat(encoder(page.uid.backtrace).toString, Utils.canonizeFileName(page.timeMillis.toString))
  }
}