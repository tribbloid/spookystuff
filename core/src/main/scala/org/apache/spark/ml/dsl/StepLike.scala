package org.apache.spark.ml.dsl

import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

/**
  * Created by peng on 24/04/16.
  */
trait StepLike extends FlowComponent {

  def id: String
  def name: String

  override def coll = StepMap(id -> this)

  override def replicate(suffix: String = ""): StepLike

  //TODO: generalized into Map[Param, Seq[String]]
  def dependencyIDs: Seq[String]

  //unlike inIDs, sequence of outIDs & parameter types (if not InputCol(s)) are not important
  def usageIDs: Set[String]
  def canBeHead: Boolean

  if (!canBeHead) assert(usageIDs.isEmpty)

  def wth(dependencyIDs: Seq[String] = dependencyIDs, usageIDs: Set[String] = usageIDs): StepLike

  override def headIDs: Seq[String] =
    if (canBeHead) Seq(id)
    else Nil

  override def leftTailIDs: Seq[String] = Seq(id)

  override def rightTailIDs: Seq[String] = Seq(id)
}

case class Step(
                 stage: NamedStage,
                 dependencyIDs: Seq[String] = Seq(),
                 usageIDs: Set[String] = Set.empty
               ) extends StepLike {

  {
    assert(this.id != PASSTHROUGH.id)
    assert(!this.dependencyIDs.contains(PASSTHROUGH.id))
    assert(!this.usageIDs.contains(PASSTHROUGH.id))
  }

  override def canBeHead: Boolean = stage.hasOutputs

  val replicas: mutable.Set[Step] = mutable.Set.empty

  override def replicate(suffix: String = ""): Step = {
    val replica = stage.replicate
    val newStage = replica.copy(name = replica.name + suffix, outputColOverride = replica.outputColOverride.map(_ + ""))

    val result = this.copy(
      stage = newStage
    )
    this.replicas += result
    result
  }

  def id = stage.id
  def name = stage.name

  def recursiveReplicas: Set[Step] = {
    val set = this.replicas.toSet
    set ++ set.flatMap(_.recursiveReplicas)
  }

  override def wth(inputIDs: Seq[String], outputIDs: Set[String]): Step = this.copy(
    dependencyIDs = inputIDs,
    usageIDs = outputIDs
  )
}

abstract class StepWrapperLike(val self: StepLike) {

  def copy(self: StepLike = self): StepWrapperLike
}

case class SimpleStepWrapper(override val self: StepLike) extends StepWrapperLike(self) {

  override def copy(self: StepLike): StepWrapperLike = SimpleStepWrapper(self)
}

trait Connector extends StepLike

case class Source(
                   name: String,
                   dataTypes: Set[DataType] = Set.empty, //used to validate & fail early when stages for different data types are appended.
                   usageIDs: Set[String] = Set.empty
                 ) extends ColumnName(name) with Connector {

  {
    assert(this.id != PASSTHROUGH.id)
    assert(!this.usageIDs.contains(PASSTHROUGH.id))
  }

  override def dependencyIDs: Seq[String] = Nil

  override def canBeHead: Boolean = true

  override def replicate(suffix: String = ""): Source = this

  def id = name

  override def wth(inputIDs: Seq[String], outputIDs: Set[String]): Source = {
    this.copy(
      usageIDs = outputIDs
    )
  }

  override def toString = "'" + name
}

case object PASSTHROUGH extends Connector {

  override def name: String = this.getClass.getSimpleName.stripSuffix("$")

  override val id: String = name //unique & cannot be referenced by others

  def dependencyIDs: Seq[String] = Nil
  def usageIDs: Set[String] = Set.empty

  override def wth(inIDs: Seq[String], outIDs: Set[String]): this.type = this

  override def canBeHead: Boolean = true

  override def replicate(suffix: String = ""): this.type = this
}