package com.tribbloids.spookystuff.caching

import java.net.URI

/**
  * Created by peng on 29/01/17.
  */
object DocCacheLevel {

  trait Value extends Serializable

  object NoCache extends Value

  trait InMemory extends Value
  object InMemory extends InMemory

  trait DFS extends Value
  object DFS extends DFS

  object All extends InMemory with DFS

  def getDefault(uriOpt: Option[URI]) = {
    val scheme = uriOpt.map(_.getScheme).getOrElse("")
    scheme match {
      case "http" | "https" =>
        All
      case "ftp" | "ftps" =>
        All
      case _ =>
        InMemory
    }
  }
}
