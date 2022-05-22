package org.apache.spark.ml.dsl

import scala.collection.mutable

trait StepGraph {

  def coll: StepMap[String, StepLike]

  def sourceColl: StepMap[String, Source] = {
    this.coll.collect {
      case (id: String, src: Source) => id -> src
    }
  }

  // generate new copy for each PipelineStage in this collection to
  // prevent one set of parameters (particularly InputCol & OutputCol) being used in multiple steps in the pipeline,
  // and attempts to set them interfere with each other
  def replicate(suffix: String = ""): StepGraph

  protected def replicateColl(
      idConversion: mutable.Map[String, String] = mutable.Map[String, String](),
      suffix: String = "",
      condition: ((String, StepLike)) => Boolean = { _: (String, StepLike) =>
        true
      } //TODO: use it!
  ): StepMap[String, StepLike] = {

    val replicatedSteps = coll.map { tuple =>
      val step = tuple._2.replicate(suffix)
      idConversion += (tuple._1 -> step.id)
      step
    }

    val newStepList = replicatedSteps.map { step =>
      step.wth(
        dependencyIDs = step.dependencyIDs.map(idConversion),
        usageIDs = step.usageIDs.map(idConversion)
      )
    }.toSeq

    val newSteps: StepMap[String, StepLike] = StepMap(newStepList.map { step =>
      step.id -> step
    }: _*)
    newSteps
  }

  def connect(fromID: String, toID: String): StepMap[String, StepLike] = {
    val from = coll(fromID)
    val to = coll(toID)

    require(from != PASSTHROUGH)
    require(to != PASSTHROUGH)

    val updatedFrom = from.wth(usageIDs = from.usageIDs + toID)
    val updatedTo = to.wth(dependencyIDs = to.dependencyIDs :+ fromID)
    val updatedSteps = coll ++ Seq(fromID -> updatedFrom, toID -> updatedTo)
    updatedSteps
  }

  //TODO: optimize
  def connectAll(fromIDs: Seq[String], toIDs: Seq[String]): StepMap[String, StepLike] = {
    var result = coll
    for (i <- fromIDs;
         j <- toIDs) {
      result = result.connect(i, j)
    }
    result
  }

  def cutInputs(id: String): StepMap[String, StepLike] = {
    val step = coll(id)
    val inSteps = step.dependencyIDs.map(coll)
    coll +
      (id -> step.wth(dependencyIDs = Nil)) ++
      inSteps.map(in => in.id -> in.wth(usageIDs = in.usageIDs - id))
  }

  def cutOutputs(id: String): StepMap[String, StepLike] = {
    val step = coll(id)
    val outSteps = step.usageIDs.map(coll)
    coll +
      (id -> step.wth(usageIDs = Set.empty)) ++
      outSteps.map(out => out.id -> out.wth(dependencyIDs = out.dependencyIDs.toBuffer - id))
  }

  def remove1(id: String): StepMap[String, StepLike] = {
    this.cutInputs(id).cutOutputs(id) - id
  }

  def remove(ids: String*): StepMap[String, StepLike] = ids.foldLeft(coll) { (coll, id) =>
    coll.remove1(id)
  }

  protected def unionImpl(coll2: StepMap[String, StepLike]): StepMap[String, StepLike] = {
    val allSteps = coll ++ coll2
    val result: StepMap[String, StepLike] = StepMap[String, StepLike](allSteps.mapValues { step =>
      val id = step.id
      step.wth(
        dependencyIDs = (coll.get(id) ++ coll2.get(id)).map(_.dependencyIDs).reduce(_ ++ _).distinct,
        usageIDs = (coll.get(id) ++ coll2.get(id)).map(_.usageIDs).reduce(_ ++ _)
      )
    }.toSeq: _*)
    result
  }

  def UU(another: StepMap[String, StepLike]): StepMap[String, StepLike] = unionImpl(another)

  implicit def stepsToView(steps: StepMap[String, StepLike]): StepMapView = new StepMapView(steps)
}

object StepGraph {

  trait MayHaveTails extends StepGraph {

    def leftTailIDs: Seq[String]
    final lazy val leftTails = leftTailIDs.map(coll)
    final lazy val leftConnectors: Seq[Connector] = leftTails.collect {
      case v: Connector => v
    }

    //root: has no src itself & is not a right tail
    final lazy val leftRoots: Seq[StepLike] = leftTails.collect {
      case v if v.dependencyIDs.isEmpty && (!rightTails.contains(v)) => v
    }

    //detached: a source that has no target, it is a tail but already end of the lineage
    //always a source
    final lazy val leftDetached: Seq[Source] = leftTails.collect {
      case v: Source if v.usageIDs.isEmpty => v
    }

    //intake: if tail is a source (rather than a step) go 1 step ahead to reach the real step
    //always a step
    final lazy val leftIntakes: Seq[Step] = leftTails.flatMap {
      case tail: Step =>
        Seq(tail)
      case source: Source =>
        source.usageIDs.map(coll).map(_.asInstanceOf[Step])
      case PASSTHROUGH => Nil
    }

    final def canConnectFromLeft: Boolean = leftIntakes.nonEmpty || leftTails.contains(PASSTHROUGH)

    def rightTailIDs: Seq[String]
    final lazy val rightTails = rightTailIDs.map(coll)
    final lazy val rightConnectors: Seq[Connector] = rightTails.collect {
      case v: Connector => v
    }
    final lazy val rightRoots: Seq[StepLike] = rightTails.collect {
      case v if v.dependencyIDs.isEmpty && (!leftTails.contains(v)) => v
    }
    final lazy val rightDetached: Seq[Source] = rightTails.collect {
      case v: Source if v.usageIDs.isEmpty => v
    }
    final lazy val rightIntakes: Seq[Step] = rightTails.flatMap {
      case tail: Step =>
        Seq(tail)
      case source: Source =>
        source.usageIDs.map(coll).map(_.asInstanceOf[Step])
      case PASSTHROUGH => Nil
    }

    final def canConnectFromRight: Boolean = rightIntakes.nonEmpty || rightTails.contains(PASSTHROUGH)

    def tailIDs: Seq[String] = leftTailIDs ++ rightTailIDs
    def tails: Seq[StepLike] = leftTails ++ rightTails
  }

  trait MayHaveHeads extends StepGraph {

    def headIDs: Seq[String]
    def fromIDs: Seq[String] = headIDs
    def headExists: Boolean = headIDs.nonEmpty

    final lazy val heads = headIDs.map(coll)
    final lazy val PASSTHROUGHOutput: Option[Connector] = heads.find(_ == PASSTHROUGH) map (_.asInstanceOf[Connector])
    final lazy val hasPASSTHROUGHOutput: Boolean = heads.contains(PASSTHROUGH)
    //all heads must have outIDs

    //TODO: separate outlet (head with outIDs) with head, which should simply denotes end of a pipe
    heads.foreach(v => require(v.canBeHead))

    protected def checkConnectivity_>(fromIDs: Seq[String], right: MayHaveTails): Unit = {
      val froms: Seq[StepLike] = fromIDs.map(coll)
      require(froms.nonEmpty, "has no from")
      require(right.canConnectFromLeft, "has no left intake")
    }

    protected def checkConnectivity_<(fromIDs: Seq[String], left: MayHaveTails): Unit = {
      val froms = fromIDs.map(coll)
      require(froms.nonEmpty, "has no from")
      require(left.canConnectFromRight, "has no right intake")
    }
  }
}
