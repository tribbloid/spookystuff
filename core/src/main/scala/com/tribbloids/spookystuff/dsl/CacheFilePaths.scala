package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.expressions.CacheFilePath
import com.tribbloids.spookystuff.utils.Utils

/**
 * Created by peng on 9/12/14.
 */

object CacheFilePaths{

  case object Flat extends CacheFilePath[String] {

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

  case object Hierarchical extends CacheFilePath[String] {

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
}