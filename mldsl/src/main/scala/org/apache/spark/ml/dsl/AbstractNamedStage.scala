package org.apache.spark.ml.dsl

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}

import scala.util.Random

case class AbstractNamedStage[+T <: PipelineStage](
                                                    stage: T,
                                                    name: String,
                                                    tags: Set[String] = Set(),
                                                    outputColOverride: Option[String] = None, //set to manually override output column name
                                                    //                       intermediate: Boolean = false //TODO: enable
                                                    _id: String = "" + Random.nextLong() //TODO: multiple Stages with same uid can't be used together?
                                                  ) {

  import org.apache.spark.ml.ShimViews._

  //create a new PipelineStage that doesn't share the same parameter
  def replicate: AbstractNamedStage[T] = {
    val result = this.copy(
      stage = this.stage.copy(ParamMap.empty).asInstanceOf[T],
      _id = "" + Random.nextLong()
    )
    result
  }

  def id = outputColOverride.getOrElse(_id)

  def outputOpt: Option[String] = stage match {
    case s: HasOutputCol =>
      Some(s.getOutputCol)
    case _ =>
      None
  }
  def hasOutputs = stage match {
    case s: HasOutputCol => true //TODO: do we really need this? implementation is inconsistent
    case _ => false
  }
  def setOutput(v: String) = {
    stage.trySetOutputCol(v)
  }

  def inputs: Seq[String] = stage match {
    case s: HasInputCol => Seq(s.getInputCol)
    case ss: HasInputCols => ss.getInputCols
    case _ => Seq()
  }

  //always have inputs
  //  def hasInputs = stage match {
  //    case s: HasInputCol => true
  //    case ss: HasInputCols => true
  //    case _ => false
  //  }
  def setInputs(v: Seq[String]) = {
    if (v.nonEmpty) { //otherwise it can be assumed that the input of this stage is already set.
      stage.trySetInputCols(v)
    }
    this
  }

  def show(
            showID: Boolean = true,
            showInputs: Boolean = true,
            showOutput: Boolean = true
          ): String = {

    val in = try {
      inputs
    }
    catch {
      case e: Throwable =>
        Seq("Pending...")
    }

    val inStr = if (showInputs) {
      in.mkString("[", ",", "]") + " > "
    }
    else ""

    val out = try {
      outputOpt
    }
    catch {
      case e: Throwable =>
        Some("Pending...")
    }

    val outStr = if (showOutput) {
      " > " + out.mkString("[", ",", "]")
    }
    else ""

    val body = name + {
      if (showID) ":" + id
      else ""
    }

    inStr + body + outStr
  }

  override def toString = show()
}