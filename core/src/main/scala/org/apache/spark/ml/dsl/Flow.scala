package org.apache.spark.ml.dsl

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
import org.apache.spark.ml.Pipeline.SharedReadWrite
import org.apache.spark.ml.dsl.utils.{FlowRelay, FlowUtils}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

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
                               condition: ((String, StepLike)) => Boolean = {_: (String, StepLike) => true} //TODO: use it!
                             ): StepMap[String, StepLike] = {

    val replicatedSteps = coll.map {
      tuple =>
        val step = tuple._2.replicate(suffix)
        idConversion += (tuple._1 -> step.id)
        step
    }

    val newStepList = replicatedSteps.map {
      step =>
        step.wth(
          dependencyIDs = step.dependencyIDs.map(idConversion),
          usageIDs = step.usageIDs.map(idConversion)
        )
    }.toSeq

    val newSteps: StepMap[String, StepLike] = StepMap(newStepList.map {
      step =>
        step.id -> step
    }: _*)
    newSteps
  }

  def connect(fromID: String, toID: String): StepMap[String, StepLike] = {
    val from = coll(fromID)
    val to = coll(toID)

    assert(from != PASSTHROUGH)
    assert(to != PASSTHROUGH)

    val updatedFrom = from.wth(usageIDs = from.usageIDs + toID)
    val updatedTo = to.wth(dependencyIDs = to.dependencyIDs :+ fromID)
    val updatedSteps = coll ++ Seq(fromID -> updatedFrom, toID -> updatedTo)
    updatedSteps
  }

  //TODO: optimize
  def connectAll(fromIDs: Seq[String], toIDs: Seq[String]): StepMap[String, StepLike] = {
    var result = coll
    for (
      i <- fromIDs;
      j <- toIDs
    ) {
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

  def remove(ids: String*): StepMap[String, StepLike] = ids.foldLeft(coll){
    (coll, id) =>
      coll.remove1(id)
  }

  protected def unionImpl(coll2: StepMap[String, StepLike]): StepMap[String, StepLike] = {
    val allSteps = coll ++ coll2
    val result: StepMap[String, StepLike] = StepMap[String, StepLike](allSteps.mapValues{
      step =>
        val id = step.id
        step.wth(
          dependencyIDs = (coll.get(id) ++ coll2.get(id)).map(_.dependencyIDs).reduce(_ ++ _).distinct,
          usageIDs = (coll.get(id) ++ coll2.get(id)).map(_.usageIDs).reduce(_ ++ _)
        )
    }.toSeq: _*)
    result
  }

  def UU (another: StepMap[String, StepLike]) = unionImpl(another)

  implicit def stepsToView(steps: StepMap[String, StepLike]): StepMapView = new StepMapView(steps)
}

class StepMapView(val coll: StepMap[String, StepLike]) extends StepGraph {

  // generate new copy for each PipelineStage in this collection to
  override def replicate(suffix: String = ""): StepMapView = new StepMapView(
    coll = this.replicateColl(suffix = suffix)
  )
}

trait MayHaveTails extends StepGraph {

  def leftTailIDs: Seq[String]
  final lazy val leftTails = leftTailIDs.map(coll)
  final lazy val leftConnectors: Seq[Connector] = leftTails.collect {
    case v: Connector => v
  }
  final lazy val leftRoots: Seq[StepLike] =  leftTails.collect {
    case v if v.dependencyIDs.isEmpty && (!rightTails.contains(v)) => v
  }
  final lazy val leftDetached: Seq[Source] =  leftTails.collect {
    case v: Source if v.usageIDs.isEmpty => v
  }
  final lazy val leftIntakes: Seq[Step] = leftTails.flatMap {
    case tail: Step =>
      Seq(tail)
    case source: Source =>
      source.usageIDs.map(coll).map(_.asInstanceOf[Step])
    case PASSTHROUGH => Nil
  }

  final def canConnectFromLeft = leftIntakes.nonEmpty || leftTails.contains(PASSTHROUGH)

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

  final def canConnectFromRight = rightIntakes.nonEmpty || rightTails.contains(PASSTHROUGH)

  def tailIDs = leftTailIDs ++ rightTailIDs
  def tails = leftTails ++ rightTails
}

trait MayHaveHeads extends StepGraph {

  def headIDs: Seq[String]
  def fromIDs: Seq[String] = headIDs
  def headExists = headIDs.nonEmpty

  final lazy val heads = headIDs.map(coll)
  final lazy val PASSTHROUGHOutput: Option[Connector] = heads.find(_ == PASSTHROUGH)map(_.asInstanceOf[Connector])
  final lazy val hasPASSTHROUGHOutput: Boolean = heads.contains(PASSTHROUGH)
  //all heads must have outIDs

  //TODO: separate outlet (head with outIDs) with head, which should simply denotes end of a pipe
  heads.foreach(v => assert(v.canBeHead))

  protected def checkConnectivity_>(fromIDs: Seq[String], right: MayHaveTails): Unit = {
    val froms = fromIDs.map(coll)
    assert(froms.nonEmpty, "has no from")
    assert(right.canConnectFromLeft, "has no left intake")
  }

  protected def checkConnectivity_<(fromIDs: Seq[String], left: MayHaveTails): Unit = {
    val froms = fromIDs.map(coll)
    assert(froms.nonEmpty, "has no from")
    assert(left.canConnectFromRight, "has no right intake")
  }
}

object FlowComponent {

  implicit def pipelineStageToStep(v: PipelineStage): Step = {
    val namedStage = NamedStage(
      v,
      v.getClass.getSimpleName,
      Set(v.getClass.getSimpleName)
    )
    Step(namedStage)
  }

  //TODO: why bother importing SQLContext.Implicits?
  implicit def pipelineStageTupleToStep(tuple: (PipelineStage, Any)): Step = {
    val namedStage = tuple match {
      case (v, s: Symbol) =>
        NamedStage(
          v,
          s.name,
          Set(s.name),
          Some(s.name)
        )
      case (v, s: Column) =>
        val clazz = s.getClass
        val name = ReflectionUtils.invoke(clazz, s, "named").asInstanceOf[NamedExpression].name
        NamedStage(
          v,
          name,
          Set(name),
          Some(name)
        )
      case (v, s: String) =>
        NamedStage (
          v,
          s,
          Set(s)
        )
    }
    Step(namedStage)
  }

  implicit def symbolToSource(s: Symbol): Source = {
    val name = s.name
    Source(name)
  }

  implicit def structFieldToSource(s: StructField): Source = {
    val name = s.name
    val dataType = s.dataType
    Source(name, Set(dataType))
  }

  //viewbound parameter
  //TODO: why bother importing SQLContext.Implicits?
  implicit def columnToSource(s: Column): Source = {
    val col: Column = s
    val clazz = col.getClass
    val name = ReflectionUtils.invoke(clazz, col, "named").asInstanceOf[NamedExpression].name
    Source(name)
  }

  def declare(flows: Flow*) = {
    flows.reduce(_ union _)
  }
}

trait FlowComponent extends MayHaveHeads with MayHaveTails {

  //validations
  {
    coll.values.foreach{
      stage =>
        if (!this.tailIDs.contains(stage.id))
          assume(stage.dependencyIDs.nonEmpty, "non-tail stage should have non-empty dependency")
    }

    if (coll.values.toSeq.contains(PASSTHROUGH)) {
      assume(hasPASSTHROUGHOutput, "PASSTHROUGH should be detached")
      assume(leftTails.contains(PASSTHROUGH), "PASSTHROUGH should be detached")
      assume(rightTails.contains(PASSTHROUGH), "PASSTHROUGH should be detached")
    }
  }

  //no replicate
  // ~> this <~
  //     |
  //     "~> right <~
  //           |
  def mergeImpl_>(fromIDs: Seq[String], right: FlowComponent): Flow = {
    checkConnectivity_>(fromIDs, right)
    val effectiveFromIDs = fromIDs.map(coll).filter(_ != PASSTHROUGH).map(_.id)
    val toIDs = right.leftIntakes.map(_.id)

    // detached port should not have any tail removed
    val newLeftTailIDs = (
      this.leftTails.flatMap{
        case PASSTHROUGH => right.leftTailIDs
        case v: StepLike => Seq(v.id)
      }
        ++ right.leftDetached.map(_.id)
      ).distinct
    val newRightTailIDs = if(right.headExists) {
      (
        right.rightTails.flatMap {
          case PASSTHROUGH => this.rightTailIDs
          case v: StepLike => Seq(v.id)
        }
          ++ this.rightRoots.map(_.id)
        ).distinct
    }
    else {
      this.rightTailIDs
    }

    val newTailIDs = newLeftTailIDs ++ newRightTailIDs
    val obsoleteIDs = (right.leftConnectors ++ this.PASSTHROUGHOutput)
      .filterNot(v => newTailIDs.contains(v.id)).map(_.id) //if in the new TailIDs, cannot be deleted which causes not found error.

    val allSteps = (coll ++ right.coll).remove(obsoleteIDs: _*)
    val newSteps = allSteps.connectAll(effectiveFromIDs, toIDs)

    val newHeadIDs =  if(right.headExists) {
      this.headIDs.toBuffer -- fromIDs ++ right.heads.flatMap {
        case PASSTHROUGH => effectiveFromIDs
        case v: StepLike => Seq(v.id)
      }
    }
    else {
      this.headIDs
    }

    val result = new Flow(
      newSteps,
      leftTailIDs = newLeftTailIDs,
      rightTailIDs = newRightTailIDs,
      headIDs = newHeadIDs
    )

    result.validateOnSources()
    result
  }

  //no replicate
  //       ~> this <~
  //           |
  // ~> left <~
  //     |
  def mergeImpl_<(fromIDs: Seq[String], left: FlowComponent): Flow = {
    checkConnectivity_<(fromIDs, left)
    val effectiveFromIDs = fromIDs.map(coll).filter(_ != PASSTHROUGH).map(_.id)
    val toIDs = left.rightIntakes.map(_.id)

    // detached port should not have any tail removed
    val newLeftTailIDs = if(left.headExists) {
      (
        left.leftTails.flatMap {
          case PASSTHROUGH => this.leftTailIDs
          case v: StepLike => Seq(v.id)
        }
          ++ this.leftRoots.map(_.id)
        ).distinct
    }
    else {
      this.leftTailIDs
    }
    val newRightTailIDs = (
      this.rightTails.flatMap {
        case PASSTHROUGH => left.rightTailIDs
        case v: StepLike => Seq(v.id)
      }
        ++ left.rightDetached.map(_.id)
      ).distinct

    val newTailIDs = newLeftTailIDs ++ newRightTailIDs
    val obsoleteIDs = (left.rightConnectors ++ this.PASSTHROUGHOutput)
      .filterNot(v => newTailIDs.contains(v.id)).map(_.id) //if in the new TailIDs, cannot be deleted which causes not found error.


    val allSteps = (coll ++ left.coll).remove(obsoleteIDs: _*)
    val newSteps = allSteps.connectAll(effectiveFromIDs, toIDs)

    val newHeadIDs = if(left.headExists) {
      this.headIDs.toBuffer -- fromIDs ++ left.heads.flatMap {
        case PASSTHROUGH => effectiveFromIDs
        case v: StepLike => Seq(v.id)
      }
    }
    else {
      this.headIDs
    }

    val result = new Flow(
      newSteps,
      leftTailIDs = newLeftTailIDs,
      rightTailIDs = newRightTailIDs,
      headIDs = newHeadIDs
    )

    result.validateOnSources()
    result
  }

  def merge_>(right: FlowComponent): Flow = mergeImpl_> (this.fromIDs, right)
  def merge(right: FlowComponent) = merge_>(right)
  def >>>(right: FlowComponent) = merge_>(right)
  def >(right: FlowComponent) = merge_>(right)

  def merge_<(left: FlowComponent): Flow = mergeImpl_<(this.fromIDs, left)
  def egrem(prev: FlowComponent) = prev.merge_<(this)
  def <<<(prev: FlowComponent) = prev.merge_<(this)
  def <(prev: FlowComponent) = prev.merge_<(this)

  def replicate(suffix: String = ""): FlowComponent

  //TODO: fast-forward handling: if right is reused for many times, ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
  def rebase_>(right: FlowComponent): Flow = {

    //    checkConnectivity_>(fromIDs, right)
    val rebasedFirst: Flow = this.mergeImpl_>(Seq(fromIDs.head), right)

    this.fromIDs.slice(1, Int.MaxValue).foldLeft(rebasedFirst){
      (flow, id) =>
        flow.mergeImpl_>(Seq(id), right.replicate())
    }
  }
  def rebase(right: FlowComponent) = rebase_>(right)
  def >=>(right: FlowComponent) = rebase_>(right)

  def rebase_<(left: FlowComponent): Flow = {

    //    checkConnectivity_<(fromIDs, left)
    val rebasedFirst: Flow = this.mergeImpl_<(Seq(fromIDs.head), left)

    this.fromIDs.slice(1, Int.MaxValue).foldLeft(rebasedFirst){
      (flow, id) =>
        flow.mergeImpl_<(Seq(id), left.replicate())
    }
  }
  def esaber(prev: FlowComponent) = prev.rebase_<(this)
  def <=<(prev: FlowComponent) = prev.rebase_<(this)

  def union(another: FlowComponent): Flow = {
    val result = Flow(
      coll = this.coll UU another.coll,
      leftTailIDs = (this.leftTailIDs ++ another.leftTailIDs).distinct,
      rightTailIDs = (this.rightTailIDs ++ another.rightTailIDs).distinct,
      headIDs = (this.headIDs ++ another.headIDs).distinct
    )
    result.validateOnSources()
    result
  }
  def U (another: FlowComponent) = union(another)

  def commit_>(right: FlowComponent): Flow = {
    val intakes = right.leftIntakes
    assert(intakes.size <= 1, "non-linear right operand, please use merge_>, rebase_> or union instead")
    intakes.headOption match {
      case Some(intake) =>
        this.rebase_>(right)
      case _ =>
        this.union(right)
    }
  }
  def commit(right: FlowComponent) = commit_>(right)
  def >->(right: FlowComponent) = commit_>(right)

  def commit_<(left: FlowComponent): Flow = {
    val intakes = left.rightIntakes
    assert(intakes.size <= 1, "non-linear left operand, please use merge_<, rebase_< or union instead")
    intakes.headOption match {
      case Some(step) =>
        this.rebase_<(left)
      case _ =>
        this.union(left)
    }
  }
  def timmoc(left: FlowComponent) = left.commit_<(this)
  def <-<(left: FlowComponent) = left.commit_<(this)

  case class StepVisualWrapper(
                                override val self: StepLike,
                                showID: Boolean = true,
                                showInputs: Boolean = true,
                                showOutput: Boolean = true,
                                showPrefix: Boolean = true
                              ) extends StepWrapperLike(self){

    def prefixes: Seq[String] = if (showPrefix) {
      val buffer = ArrayBuffer[String]()
      if (FlowComponent.this.headIDs contains self.id) buffer += "HEAD"
      //      else {
      val isLeftTail = FlowComponent.this.leftTailIDs contains self.id
      val isRightTail = FlowComponent.this.rightTailIDs contains  self.id
      if (isLeftTail && isRightTail) buffer += "TAIL"
      else {
        if (isLeftTail) buffer += "TAIL>"
        if (isRightTail) buffer += "<TAIL"
      }
      //      }
      buffer
    }
    else Nil

    override def toString = prefixes.map("(" + _ +")").mkString("") + " " + {
      self match {
        case v: Step => v.stage.show(showID, showInputs, showOutput)
        case v: Connector => "["+v.id+"]"
      }
    }

    override def copy(self: StepLike): StepWrapperLike = StepVisualWrapper(self, showID, showInputs, showOutput, showPrefix)
  }
  //TODO: not optimized, children are repeatedly created when calling .path
  //TODO: use mapChildren to recursively get TreeNode[(Seq[String] -> Tree)] efficiently
  case class ForwardNode(
                          wrapper: StepWrapperLike
                        ) extends StepTreeNode[ForwardNode] {

    //    def prefix = if (this.children.nonEmpty) "v "
    def prefix = if (this.children.nonEmpty) "> "
    else "> "

    override def nodeName: String =  prefix + super.nodeName

    override val self: StepLike = wrapper.self

    override lazy val children: Seq[ForwardNode] = {
      self.usageIDs.map {
        id =>
          FlowComponent.this.coll(id)
      }
        .toList
        .sortBy(_.name)
        .map{
          v =>
            ForwardNode(wrapper.copy(
              v
            ))
        }
    }
  }

  case class BackwardNode(
                           wrapper: StepWrapperLike
                         ) extends StepTreeNode[BackwardNode] {

    //    def prefix = if (this.children.nonEmpty) "^ "
    def prefix = if (this.children.nonEmpty) "< "
    else "< "

    override def nodeName: String = prefix + super.nodeName

    override val self: StepLike = wrapper.self

    override lazy val children: Seq[BackwardNode] = {
      self.dependencyIDs.map {
        id =>
          FlowComponent.this.coll(id)
      }
        .map(v => BackwardNode(wrapper.copy(v)))
    }
  }

  def disambiguateNames[T <: PipelineStage](ids_MultiPartNames: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    val ids_disambiguatedNames = ids_MultiPartNames
      .groupBy(_._2)
      .map {
        tuple =>
          val coNamed = tuple._2
          val revised: Map[String, Seq[String]] = if (coNamed.size > 1) {
            coNamed.zipWithIndex.map {
              withIndex =>
                val id = withIndex._1._1
                val names = withIndex._1._2
                val lastName = names.last
                val withSuffix = lastName + withIndex._2
                val namesWithSuffix = names.slice(0, names.size - 1) :+ withSuffix
                id -> namesWithSuffix
            }
          }
          else coNamed
          revised
      }
      .reduce(_ ++ _)
    ids_disambiguatedNames
  }

  //this operation IS stateful & destructive, any other options?
  def propagateCols[T <: PipelineStage](compaction: PathCompaction): Unit = {
    val ids_MultiPartNames = coll.mapValues(v => this.BackwardNode(StepVisualWrapper(v)).mergedPath)

    val lookup = compaction(ids_MultiPartNames.values.toSet)
    val compactNames = lookup.values.toSeq
    assert(compactNames.size == compactNames.distinct.size)

    val ids_compactNames = ids_MultiPartNames.mapValues(lookup)
    val ids_disambiguatedNames = disambiguateNames(ids_compactNames)
    val disambiguatedNames = ids_disambiguatedNames.values.toSeq
    assert(disambiguatedNames.size == disambiguatedNames.distinct.size)

    val ids_cols = ids_disambiguatedNames.mapValues(_.mkString("$"))

    this.coll.foreach {
      case (_, step: Step) =>
        val stage = step.stage

        if (stage.hasOutputs) {
          val outCol = ids_cols(step.id)
          stage.setOutput(outCol)
        }

        val inCols = step.dependencyIDs.map(ids_cols)
        stage.setInputs(inCols)
      case _ => //do nothing
    }
  }

  // algorithm that starts from tail and gradually append by exploring all directed edges,
  // it only append steps that has all dependencies in the list
  // it is fast and can be used whenever a new Flow is constructed and has typed sources.
  def buildStagesImpl[T <: PipelineStage](
                                           compaction: PathCompaction = Flow.DEFAULT_COMPACTION,
                                           fieldsEvidenceOpt: Option[Array[StructField]] = None, //set this to make pipeline adaptive to df being transformed.
                                           adaptation: SchemaAdaptation = Flow.DEFAULT_SCHEMA_ADAPTATION
                                         ): Pipeline = {
    propagateCols(compaction)

    val stageBuffer = ArrayBuffer[T]()

    val effectiveAdaptation = fieldsEvidenceOpt match {
      case None => SchemaAdaptations.Force
      case _ => adaptation
    }

    // has to preserve order of insertion.
    val queue: StepBuffer[String, StepLike] = effectiveAdaptation match {
      case SchemaAdaptations.Force =>
        StepBuffer.newBuilder[String, StepLike].++= {
          sourceColl
        }.result()
      case _ =>
        StepBuffer.newBuilder[String, StepLike].++= {
          fieldsEvidenceOpt.get.map {
            field =>
              val source = Source(field.name, dataTypes = Set(field.dataType))
              source.id -> source
          }
        }.result()
    }

    // if nonEmpty, validate sink in each iteration by performing a PipelineStage.transformSchema
    var currentSchemaOpt: Option[StructType] = effectiveAdaptation match {
      case _: SchemaAdaptations.TypeUnsafe =>
        None
      case _ =>
        fieldsEvidenceOpt.map {
          fields =>
            new StructType(fields)
        }
    }

    val allSteps = this.coll.collect {
      case (id: String, step: Step) => id -> step
    }
    val warehouse: StepBuffer[String, Step] = {
      StepBuffer.newBuilder.++= {
        allSteps
      }.result()
    }

    // has 2 resolutions:
    // if dependency is fulfilled and pass the schema check, return it
    // if dependency is fulfilled but but doesn't pass schema check (if any), do not return it/fail fast depending on adaptation

    def nextOptImpl(): Option[(String, Step)] = {
      val candidate = warehouse.find {
        v =>
          if (v._2.dependencyIDs.forall(queue.contains)) {

            // schema validation here.
            try {
              currentSchemaOpt = currentSchemaOpt.map {
                schema =>
                  v._2.stage.stage.transformSchema(schema)
              }
              true
            }
            catch {
              case e: Throwable =>
                effectiveAdaptation match {
                  case _: SchemaAdaptations.FailOnInconsistentSchema =>
                    throw e
                  case SchemaAdaptations.IgnoreIrrelevant =>
                    false
                  case _ =>
                    sys.error("impossible")
                }
            }
          }
          else false
      }
      candidate
    }

    var nextOpt = nextOptImpl()
    while (nextOpt.nonEmpty) {
      val next = nextOpt.get

      stageBuffer += next._2.stage.stage.asInstanceOf[T]

      warehouse -= next._1
      queue += next

      nextOpt = nextOptImpl()
    }

    val result = new Pipeline()
      .setStages(stageBuffer.toArray)

    effectiveAdaptation match {
      case _: SchemaAdaptations.IgnoreIrrelevant =>
        // add assumed sources into fulfilled dependency list and try again to exhaust warehouse
        queue ++= this.coll.collect {
          case (id: String, src: Source) => id -> src
        }
        currentSchemaOpt = None //typeCheck no longer required

        nextOpt = nextOptImpl()
        while (nextOpt.nonEmpty) {
          val next = nextOpt.get

          warehouse -= next._1
          queue += next

          nextOpt = nextOptImpl()
        }
      case _ =>
        require(
          warehouse.isEmpty,
          s"Missing dependency:\n" + warehouse.values.map(_.stage).mkString("\n")
        )
    }

    assert (
      warehouse.isEmpty,
      "Cyclic pipeline stage dependency:\n" + warehouse.values.map(_.stage).mkString("\n")
    )

    result
  }

  def build(
             compaction: PathCompaction = Flow.DEFAULT_COMPACTION,
             fieldsEvidence: Array[StructField] = null, //set this to make pipeline adaptive to df being transformed.
             schemaEvidence: StructType = null, //set this to make pipeline adaptive to df being transformed.
             dfEvidence: DataFrame = null, //set this to make pipeline adaptive to df being transformed.
             adaptation: SchemaAdaptation = Flow.DEFAULT_SCHEMA_ADAPTATION
           ): Pipeline = {

    buildStagesImpl[PipelineStage](
      compaction,
      Option(fieldsEvidence)
        .orElse{
          Option(schemaEvidence).map(_.fields)
        }
        .orElse{
          Option(dfEvidence).map(_.schema.fields)
        },
      adaptation
    )
  }

  def buildModel(
                  compaction: PathCompaction = Flow.DEFAULT_COMPACTION,
                  fieldsEvidence: Array[StructField] = null, //set this to make pipeline adaptive to df being transformed.
                  schemaEvidence: StructType = null, //set this to make pipeline adaptive to df being transformed.
                  dfEvidence: DataFrame = null, //set this to make pipeline adaptive to df being transformed.
                  adaptation: SchemaAdaptation = Flow.DEFAULT_SCHEMA_ADAPTATION
                ): PipelineModel = {

    coll.foreach{
      case (_, v: Step) => assert(v.stage.stage.isInstanceOf[Transformer])
      case _ =>
    }

    val pipeline = buildStagesImpl[Transformer](
      compaction,
      Option(fieldsEvidence)
        .orElse{
          Option(schemaEvidence).map(_.fields)
        }
        .orElse{
          Option(dfEvidence).map(_.schema.fields)
        },
      adaptation
    )

    new PipelineModel(pipeline.uid, pipeline.getStages.map(_.asInstanceOf[Transformer]))
      .setParent(pipeline)
  }

  // preemptive uildStage with type safety check, always fail fast
  // use to validate in FlowComponent Constructor and fail early.
  // stateless, replicate self before applying propagateCols, stateful changes are discarded.
  protected def validateOnSchema(fieldsEvidence: Array[StructField]): Unit = {
    this.replicate().buildStagesImpl[PipelineStage](
      Flow.COMPACTION_FOR_TYPECHECK,
      fieldsEvidenceOpt = Some(fieldsEvidence),
      adaptation = Flow.SCHEMA_ADAPTATION_FOR_TYPECHECK
    )
  }

  protected def validateOnSources(): Unit = {
    val fields: List[Set[StructField]] = this.sourceColl
      .filter(_._2.dataTypes.nonEmpty)
      .values
      .map{
        source =>
          source.dataTypes.map(t => new StructField(source.name, t))
      }
      .toList

    val cartesian = FlowUtils.cartesianProductSet(fields)
    val schemas = cartesian.map(v => new StructType(v.toArray))
    schemas.foreach{
      schema =>
        if (schema.fields.nonEmpty) validateOnSchema(schema.fields)
    }
  }

  def showForwardTree(
                       tails: Seq[StepLike],
                       showID: Boolean,
                       showInputs: Boolean,
                       showOutput: Boolean,
                       showPrefix: Boolean
                     ): String = {
    tails.map {
      tail =>
        val prettyTail = StepVisualWrapper(tail, showID, showInputs, showOutput, showPrefix)
        val treeNode = ForwardNode(prettyTail)
        treeNode.treeString
    }
      .mkString("")
  }

  def showBackwardTree(
                        heads: Seq[StepLike],
                        showID: Boolean,
                        showInputs: Boolean,
                        showOutput: Boolean,
                        showPrefix: Boolean
                      ): String = {
    heads.map {
      head =>
        val prettyHead = StepVisualWrapper(head, showID, showInputs, showOutput, showPrefix)
        val treeNode = BackwardNode(prettyHead)
        treeNode.treeString
    }
      .mkString("")
  }

  protected final val mirrorImgs = List(
    'v' -> '^',
    '┌' -> '└',
    '┘' -> '┐',
    '┬' -> '┴'
  )

  protected def flipChar(char: Char): Char = {
    mirrorImgs.find(_._1 == char).map(_._2).getOrElse{
      mirrorImgs.find(_._2 == char).map(_._1).getOrElse{
        char
      }
    }
  }

  protected final val layoutPrefs = LayoutPrefsImpl(unicode = true, explicitAsciiBends = false)
  def showASCIIArt(
                    forward: Boolean,
                    showID: Boolean,
                    showInputs: Boolean,
                    showOutput: Boolean,
                    showPrefix: Boolean
                  ): String = {

    val prettyColl = coll.mapValues {
      v =>
        StepVisualWrapper(v, showID, showInputs, showOutput, showPrefix)
    }

    val vertices: Set[StepVisualWrapper] = prettyColl.values.toSet
    val edges: List[(StepVisualWrapper, StepVisualWrapper)] = prettyColl.values.toList.flatMap{
      v =>
        v.self.usageIDs.map(prettyColl).map(vv => v -> vv)
    }
    val graph: Graph[StepVisualWrapper] = Graph[StepVisualWrapper](vertices = vertices, edges = edges)

    val forwardStr = GraphLayout.renderGraph(graph, layoutPrefs = layoutPrefs)
    if (forward) forwardStr
    else {
      forwardStr
        .split('\n')
        .reverse
        .mkString("\n")
        .map(flipChar)
    }
  }

  def show(
            showID: Boolean = true,
            showInputs: Boolean = true,
            showOutput: Boolean = true,
            showPrefix: Boolean = true,
            forward: Boolean = true,
            asciiArt: Boolean = false,
            compactionOpt: Option[PathCompaction] = Some(Flow.DEFAULT_COMPACTION)
          ) = {
    compactionOpt.foreach(this.propagateCols)

    if (!asciiArt){
      if (forward) {

        "\\ left >\n" + showForwardTree(leftTails, showID, showInputs, showOutput, showPrefix) +
          "/ right <\n" + showForwardTree(rightTails, showID, showInputs, showOutput, showPrefix)
      }
      else {

        showBackwardTree(this.heads, showID, showInputs, showOutput, showPrefix)
      }
    }
    else {
      showASCIIArt(forward, showID, showInputs, showOutput, showPrefix)
    }
  }
}

object Flow extends MLReadable[Flow] {

  final val DEFAULT_COMPACTION: PathCompaction = Compactions.PruneDownPath
  final val DEFAULT_SCHEMA_ADAPTATION: SchemaAdaptation = SchemaAdaptations.FailFast

  final val COMPACTION_FOR_TYPECHECK: PathCompaction = Compactions.DoNotCompact
  final val SCHEMA_ADAPTATION_FOR_TYPECHECK: SchemaAdaptation = SchemaAdaptations.IgnoreIrrelevant_ValidateSchema

  def apply(v: PipelineStage): Step = v
  def apply(tuple: (PipelineStage, Any)): Step = tuple
  def apply(s: Symbol): Source = s
  def apply(s: StructField): Source = s

//  class FlowWriter(flow: Flow) extends MLWriter {
//
//    SharedReadWrite.validateStages(flow.stages)
//
//    def stageDeclarationJSON: JArray = {
//      val namedStages: Seq[NamedStage] = flow.coll.values.collect {
//        case st: Step => st.stage
//      }.toSeq
//
//      val stageJSONs: Seq[JObject] = namedStages.map {
//        ns =>
//          val obj: JObject = (
//            ("id" -> ns.id)
//              ~ ("uid" -> ns.stage.uid)
//              ~ ("name" -> ns.name)
//              ~ ("tags" -> ns.tags.map {
//              tag =>
//                "tag" -> tag
//            })
//              ~ ("outputColOverride" -> ns.outputColOverride)
//            )
//          obj
//      }
//
//      val result = stageJSONs.map {
//        v =>
//          v
//      }
//
//      result: JArray
//    }
//
//    def forwardTreeJSON: JValue = {
//
//      val tailWrappers = flow.tails.map(SimpleStepWrapper)
//      val forwardTrees = tailWrappers.map(flow.ForwardNode)
//
//      val forwardTreesJVs = forwardTrees.map(_.jsonValue)
//
//      val result = forwardTreesJVs.map {
//        v =>
//          "stage" -> v
//      }
//
//      result: JValue
//    }
//
//    val basicMetadata: JObject =
//      ("timestamp" -> System.currentTimeMillis()) ~
//        ("sparkVersion" -> sc.version) ~
//        ("uid" -> flow.uid) ~
//        ("org" -> "sc1")
//    //      ("paramMap" -> jsonParams)
//
//    val graphMetadata: JObject =
//      "headIDs" ->
//        ("headID" -> flow.headIDs)
//
//    def json: JValue = {
//      flow.propagateCols(Compactions.PruneDownPath)
//
//      "flow" -> (
//        basicMetadata
//          ~ ("declarations" ->
//          ("stage" -> stageDeclarationJSON)
//          )
//          ~ ("flowLines" ->
//          ("flowLine" -> forwardTreeJSON)
//          )
//          ~ graphMetadata
//        )
//    }
//
//    def compactJSON = compact(render(json))
//    def prettyJSON = pretty(render(json))
//
//    def xmlNode = Xml.toXml(json)
//    def compactXML = xmlNode.toString()
//    def prettyXML = Const.modelXmlPrinter.formatNodes(xmlNode)
//
//    override protected def saveImpl(path: String): Unit = {
//
//      val resolver = HDFSResolver(sc.hadoopConfiguration)
//
//      resolver.output(path, overwrite = true){
//        os =>
//          os.write(prettyJSON.getBytes("UTF-8"))
//      }
//    }
//  }

  object FlowReader extends MLReader[Flow] {

    /** Checked against metadata when loading model */
    private val className = classOf[Flow].getName

    override def load(path: String): Flow = {
      val (
        uid: String,
        stages: Array[PipelineStage]
        ) = SharedReadWrite.load(className, sc, path) //TODO: not sure if it can be reused

      null
    }
  }

  @org.apache.spark.annotation.Since("1.6.0")
  override def read = FlowRelay.toMLReader
}

//TODO: should I be using decorator/mixin?
case class Flow(
                 coll: StepMap[String, StepLike],
                 leftTailIDs: Seq[String],
                 rightTailIDs: Seq[String],
                 headIDs: Seq[String],
                 fromIDsOpt: Option[Seq[String]] = None //overrriden by using "from" function
               ) extends FlowComponent with MLWritable with FlowRelay.ToReprMixin {

  override def fromIDs = fromIDsOpt.getOrElse(headIDs)

  lazy val stages = coll.values.collect {
    case st: Step =>
      st.stage.stage
  }.toArray.distinct

  def from(name: String) = {
    val newFromIDs = coll.values.filter(_.name == name).map(_.id).toSeq
    this.copy(
      fromIDsOpt = Some(newFromIDs)
    )
  }
  def >-(name: String) = from(name)

  def and(name: String) = {
    val newFromIDs = coll.values.filter(_.name == name).map(_.id).toSeq
    this.copy(
      fromIDsOpt = Some(this.fromIDs ++ newFromIDs)
    )
  }
  def <>-(name: String) = and(name)

  def replicate(suffix: String = ""): Flow = {

    val idConversion = mutable.Map[String, String]()

    val newSteps: StepMap[String, StepLike] = replicateColl(suffix = suffix, idConversion = idConversion)

    new Flow(
      newSteps,
      leftTailIDs.map(idConversion),
      rightTailIDs.map(idConversion),
      headIDs.map(idConversion),
      this.fromIDsOpt
    )
  }

  @org.apache.spark.annotation.Since("1.6.0")
  override def write = {
    this.propagateCols(Flow.DEFAULT_COMPACTION)

    FlowRelay.toMLWriter(this)
  }
}