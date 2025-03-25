package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.HasTrace.NoStateChange
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.*

/**
  * Export a page from the browser or http client the page an be anything including HTML/XML file, image, PDF file or
  * JSON string.
  */
@SerialVersionUID(564570120183654L)
abstract class Export extends MayExport.Named with NoStateChange {

  override def exportNames: Set[String] = Set(name)

  def filter: DocFilter = DocFilter.Bypass

  final def doExe(agent: Agent): Seq[Observation] = {
    val results = doExeNoName(agent)
    results.map {
      case doc: Doc =>
        val result = filter.apply(doc -> agent)
        result

      case other: Observation =>
        other
    }
  }

  def doExeNoName(agent: Agent): Seq[Observation]
}

object Export {}
