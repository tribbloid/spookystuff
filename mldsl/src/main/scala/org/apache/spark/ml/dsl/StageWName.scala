package org.apache.spark.ml.dsl

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}

case class StageWName[+T <: PipelineStage](
    stage: T,
    name: String,
    tags: Set[String] = Set(),
    outputColOverride: Option[String] = None //set to manually override output column name
    //                       intermediate: Boolean = false //TODO: enable
    //    _id: String = "" + Random.nextLong() //TODO: multiple Stages with same uid can't be used together?
) {

  import ShimViews._

  //create a new PipelineStage that doesn't share the same parameter
  def replicate: StageWName[T] = {

    val replica = DSLUtils.replicateStage(stage)

    val result = this.copy(
      replica
    )
    result
  }

  def uid: String = outputColOverride.getOrElse(stage.uid)

  def outputOpt: Option[String] = stage match {
    case s: HasOutputCol =>
      Some(s.getOutputCol)
    case _ =>
      None
  }
  def hasOutputs: Boolean = stage match {
    case s: HasOutputCol => true //TODO: do we really need this? implementation is inconsistent
    case _               => false
  }
  def setOutput(v: String): Params = {
    stage.trySetOutputCol(v)
  }

  def inputs: Seq[String] = stage match {
    case s: HasInputCol   => Seq(s.getInputCol)
    case ss: HasInputCols => ss.getInputCols
    case _                => Seq()
  }

  //always have inputs
  //  def hasInputs = stage match {
  //    case s: HasInputCol => true
  //    case ss: HasInputCols => true
  //    case _ => false
  //  }
  def setInputs(v: Seq[String]): StageWName[T] = {
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
    } catch {
      case e: Exception =>
        Seq(s"Pending... ${e.getMessage}")
    }

    val inStr = if (showInputs) {
      in.mkString("[", ",", "]") + " > "
    } else ""

    val out = try {
      outputOpt
    } catch {
      case e: Exception =>
        Some(s"Pending... ${e.getMessage}")
    }

    val outStr = if (showOutput) {
      " > " + out.mkString("[", ",", "]")
    } else ""

    val body = name + {
      if (showID) ":" + uid
      else ""
    }

    inStr + body + outStr
  }

  override def toString: String = show()
}
