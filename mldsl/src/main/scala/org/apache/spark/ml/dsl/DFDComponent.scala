package org.apache.spark.ml.dsl

import org.apache.spark.ml.dsl.StepGraph.{MayHaveHeads, MayHaveTails}
import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import org.scalameta.ascii.graph.Graph
import org.scalameta.ascii.layout.GraphLayout
import org.scalameta.ascii.layout.prefs.LayoutPrefsImpl

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object DFDComponent {

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
        val name = UnsafeUtils.invoke(clazz, s, "named").asInstanceOf[NamedExpression].name
        NamedStage(
          v,
          name,
          Set(name),
          Some(name)
        )
      case (v, s: String) =>
        NamedStage(
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
    val name = UnsafeUtils.invoke(clazz, col, "named").asInstanceOf[NamedExpression].name
    Source(name)
  }

  def declare(flows: DFD*): DFD = {
    flows.reduce(_ union _)
  }
}

trait DFDComponent extends MayHaveHeads with MayHaveTails {

  //validations
  {
    coll.values.foreach { stage =>
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
  def composeImpl_>(fromIDs: Seq[String], right: DFDComponent): DFD = {
    checkConnectivity_>(fromIDs, right)
    val effectiveFromIDs = fromIDs.map(coll).filter(_ != PASSTHROUGH).map(_.id)
    val toIDs = right.leftIntakes.map(_.id)

    // detached port should not have any tail removed
    val newLeftTailIDs = (
      this.leftTails.flatMap {
        case PASSTHROUGH => right.leftTailIDs
        case v: StepLike => Seq(v.id)
      }
        ++ right.leftDetached.map(_.id)
    ).distinct
    val newRightTailIDs = if (right.headExists) {
      (
        right.rightTails.flatMap {
          case PASSTHROUGH => this.rightTailIDs
          case v: StepLike => Seq(v.id)
        }
          ++ this.rightRoots.map(_.id)
      ).distinct
    } else {
      this.rightTailIDs
    }

    val newTailIDs = newLeftTailIDs ++ newRightTailIDs
    val obsoleteIDs = (right.leftConnectors ++ this.PASSTHROUGHOutput)
      .filterNot(v => newTailIDs.contains(v.id))
      .map(_.id) //if in the new TailIDs, cannot be deleted which causes not found error.

    val allSteps = (coll ++ right.coll).remove(obsoleteIDs: _*)
    val newSteps = allSteps.connectAll(effectiveFromIDs, toIDs)

    val newHeadIDs = if (right.headExists) {
      this.headIDs.toBuffer --= fromIDs ++= right.heads.flatMap {
        case PASSTHROUGH => effectiveFromIDs
        case v: StepLike => Seq(v.id)
      }
    } else {
      this.headIDs
    }

    val result = new DFD(
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
  def composeImpl_<(fromIDs: Seq[String], left: DFDComponent): DFD = {
    checkConnectivity_<(fromIDs, left)
    val effectiveFromIDs = fromIDs.map(coll).filter(_ != PASSTHROUGH).map(_.id)
    val toIDs = left.rightIntakes.map(_.id)

    // detached port should not have any tail removed

    val newLeftTailIDs: Seq[String] = if (left.headExists) {
      (
        left.leftTails.flatMap {
          case PASSTHROUGH => this.leftTailIDs
          case v: StepLike => Seq(v.id)
        }
          ++ this.leftRoots.map(_.id)
      ).distinct
    } else {
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
      .filterNot(v => newTailIDs.contains(v.id))
      .map(_.id) //if in the new TailIDs, cannot be deleted which causes not found error.

    val allSteps = (coll ++ left.coll).remove(obsoleteIDs: _*)
    val newSteps = allSteps.connectAll(effectiveFromIDs, toIDs)

    val newHeadIDs = if (left.headExists) {
      this.headIDs.toBuffer -- fromIDs ++ left.heads.flatMap {
        case PASSTHROUGH => effectiveFromIDs
        case v: StepLike => Seq(v.id)
      }
    } else {
      this.headIDs
    }

    val result = new DFD(
      newSteps,
      leftTailIDs = newLeftTailIDs,
      rightTailIDs = newRightTailIDs,
      headIDs = newHeadIDs
    )

    result.validateOnSources()
    result
  }

  def compose_>(right: DFDComponent): DFD = composeImpl_>(this.fromIDs, right)
  def compose(right: DFDComponent): DFD = compose_>(right)
  def :>>(right: DFDComponent): DFD = compose_>(right)
//  def >(right: FlowComponent) = compose_>(right)

  //TODO: fast-forward handling: if right is reused for many times, ensure that only the part that doesn't overlap with this got duplicated (conditional duplicate)
  def mapHead_>(right: DFDComponent): DFD = {

    //    checkConnectivity_>(fromIDs, right)
    val firstResult: DFD = this.composeImpl_>(Seq(fromIDs.head), right)

    this.fromIDs.slice(1, Int.MaxValue).foldLeft(firstResult) { (flow, id) =>
      flow.composeImpl_>(Seq(id), right.replicate())
    }
  }
  def mapHead(right: DFDComponent): DFD = mapHead_>(right)
  def :=>>(right: DFDComponent): DFD = mapHead_>(right)

  def compose_<(left: DFDComponent): DFD = composeImpl_<(this.fromIDs, left)
  def <<:(left: DFDComponent): DFD = compose_<(left)
//  def <(left: FlowComponent) = compose_<(left)

  def replicate(suffix: String = ""): DFDComponent

  def mapHead_<(left: DFDComponent): DFD = {

    //    checkConnectivity_<(fromIDs, left)
    val firstResult: DFD = this.composeImpl_<(Seq(fromIDs.head), left)

    this.fromIDs.slice(1, Int.MaxValue).foldLeft(firstResult) { (flow, id) =>
      flow.composeImpl_<(Seq(id), left.replicate())
    }
  }
  def <<=:(prev: DFDComponent): DFD = mapHead_<(prev)

  def union(another: DFDComponent): DFD = {
    val result = DFD(
      coll = this.coll UU another.coll,
      leftTailIDs = (this.leftTailIDs ++ another.leftTailIDs).distinct,
      rightTailIDs = (this.rightTailIDs ++ another.rightTailIDs).distinct,
      headIDs = (this.headIDs ++ another.headIDs).distinct
    )
    result.validateOnSources()
    result
  }
  def U(another: DFDComponent): DFD = union(another)

  def append_>(right: DFDComponent): DFD = {
    val intakes = right.leftIntakes
    require(intakes.size <= 1, "non-linear right operand, please use compose_>, mapHead_> or union instead")
    intakes.headOption match {
      case Some(intake) =>
        this.mapHead_>(right)
      case _ =>
        this.union(right)
    }
  }
  def append(right: DFDComponent): DFD = append_>(right)
  def :->(right: DFDComponent): DFD = append_>(right)

  def append_<(left: DFDComponent): DFD = {
    val intakes = left.rightIntakes
    require(intakes.size <= 1, "non-linear left operand, please use compose_<, mapHead_< or union instead")
    intakes.headOption match {
      case Some(step) =>
        this.mapHead_<(left)
      case _ =>
        this.union(left)
    }
  }
  def <-:(left: DFDComponent): DFD = append_<(left)

  case class StepVisualWrapper(
      override val self: StepLike,
      showID: Boolean = true,
      showInputs: Boolean = true,
      showOutput: Boolean = true,
      showPrefix: Boolean = true
  ) extends StepWrapperLike(self) {

    def prefixes: Seq[String] =
      if (showPrefix) {
        val buffer = ArrayBuffer[String]()
        if (DFDComponent.this.headIDs contains self.id) buffer += "HEAD"
        //      else {
        val isLeftTail = DFDComponent.this.leftTailIDs contains self.id
        val isRightTail = DFDComponent.this.rightTailIDs contains self.id
        if (isLeftTail && isRightTail) buffer += "TAIL"
        else {
          if (isLeftTail) buffer += "TAIL>"
          if (isRightTail) buffer += "<TAIL"
        }
        //      }
        buffer
      } else Nil

    override def toString: String = prefixes.map("(" + _ + ")").mkString("") + " " + {
      self match {
        case v: Step      => v.stage.show(showID, showInputs, showOutput)
        case v: Connector => "[" + v.id + "]"
      }
    }

    override def copy(self: StepLike): StepWrapperLike =
      StepVisualWrapper(self, showID, showInputs, showOutput, showPrefix)
  }
  //TODO: not optimized, children are repeatedly created when calling .path
  //TODO: use mapChildren to recursively get TreeNode[(Seq[String] -> Tree)] efficiently
  case class ForwardNode(
      wrapper: StepWrapperLike
  ) extends StepTreeNode[ForwardNode] {

    //    def prefix = if (this.children.nonEmpty) "v "
    def prefix: String =
      if (this.children.nonEmpty) "> "
      else "> "

    override def nodeName: String = prefix + super.nodeName

    override val self: StepLike = wrapper.self

    override lazy val children: Seq[ForwardNode] = {
      self.usageIDs
        .map { id =>
          DFDComponent.this.coll(id)
        }
        .toList
        .sortBy(_.name)
        .map { v =>
          ForwardNode(
            wrapper.copy(
              v
            ))
        }
    }
  }

  case class BackwardNode(
      wrapper: StepWrapperLike
  ) extends StepTreeNode[BackwardNode] {

    //    def prefix = if (this.children.nonEmpty) "^ "
    def prefix: String =
      if (this.children.nonEmpty) "< "
      else "< "

    override def nodeName: String = prefix + super.nodeName

    override val self: StepLike = wrapper.self

    override lazy val children: Seq[BackwardNode] = {
      self.dependencyIDs
        .map { id =>
          DFDComponent.this.coll(id)
        }
        .map(v => BackwardNode(wrapper.copy(v)))
    }
  }

  def disambiguateNames[T <: PipelineStage](ids_MultiPartNames: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    val ids_disambiguatedNames = ids_MultiPartNames
      .groupBy(_._2)
      .map { tuple =>
        val coNamed = tuple._2
        val revised: Map[String, Seq[String]] = if (coNamed.size > 1) {
          coNamed.zipWithIndex.map { withIndex =>
            val id = withIndex._1._1
            val names = withIndex._1._2
            val lastName = names.last
            val withSuffix = lastName + withIndex._2
            val namesWithSuffix = names.slice(0, names.size - 1) :+ withSuffix
            id -> namesWithSuffix
          }
        } else coNamed
        revised
      }
      .reduce(_ ++ _)
    ids_disambiguatedNames
  }

  //this operation IS stateful & destructive, any other options?
  //TODO: should generate deep copy to become stateless
  def propagateCols[T <: PipelineStage](compaction: PathCompaction): Unit = {
    val ids_MultiPartNames = coll.mapValues(v => this.BackwardNode(StepVisualWrapper(v)).mergedPath)

    val lookup = compaction(ids_MultiPartNames.values.toSet)
    val compactNames = lookup.values.toSeq
    require(compactNames.size == compactNames.distinct.size)

    val ids_compactNames = ids_MultiPartNames.mapValues(lookup)
    val ids_disambiguatedNames = disambiguateNames(ids_compactNames)
    val disambiguatedNames = ids_disambiguatedNames.values.toSeq
    require(disambiguatedNames.size == disambiguatedNames.distinct.size)

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
      compaction: PathCompaction = DFD.DEFAULT_COMPACTION,
      fieldsEvidenceOpt: Option[Array[StructField]] = None, //set this to make pipeline adaptive to df being transformed.
      adaptation: SchemaAdaptation = DFD.DEFAULT_SCHEMA_ADAPTATION
  ): Pipeline = {
    propagateCols(compaction)

    val stageBuffer = ArrayBuffer[T]()

    val effectiveAdaptation = fieldsEvidenceOpt match {
      case None => SchemaAdaptation.Force
      case _    => adaptation
    }

    // has to preserve order of insertion.
    val queue: StepBuffer[String, StepLike] = effectiveAdaptation match {
      case SchemaAdaptation.Force =>
        StepBuffer
          .newBuilder[String, StepLike]
          .++= {
            sourceColl
          }
          .result()
      case _ =>
        StepBuffer
          .newBuilder[String, StepLike]
          .++= {
            fieldsEvidenceOpt.get.map { field =>
              val source = Source(field.name, dataTypes = Set(field.dataType))
              source.id -> source
            }
          }
          .result()
    }

    // if nonEmpty, validate sink in each iteration by performing a PipelineStage.transformSchema
    var currentSchemaOpt: Option[StructType] = effectiveAdaptation match {
      case _: SchemaAdaptation.TypeUnsafe =>
        None
      case _ =>
        fieldsEvidenceOpt.map { fields =>
          new StructType(fields)
        }
    }

    val allSteps = this.coll.collect {
      case (id: String, step: Step) => id -> step
    }
    val warehouse: StepBuffer[String, Step] = {
      StepBuffer.newBuilder
        .++= {
          allSteps
        }
        .result()
    }

    // has 2 resolutions:
    // if dependency is fulfilled and pass the schema check, return it
    // if dependency is fulfilled but but doesn't pass schema check (if any), do not return it/fail fast depending on adaptation

    def nextOptImpl(): Option[(String, Step)] = {
      val candidate = warehouse.find { v =>
        if (v._2.dependencyIDs.forall(queue.contains)) {

          // schema validation here.
          try {
            currentSchemaOpt = currentSchemaOpt.map { schema =>
              v._2.stage.stage.transformSchema(schema)
            }
            true
          } catch {
            case e: Exception =>
              effectiveAdaptation match {
                case _: SchemaAdaptation.FailOnInconsistentSchema =>
                  throw e
                case SchemaAdaptation.IgnoreIrrelevant =>
                  false
                case _ =>
                  sys.error("impossible")
              }
          }
        } else false
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
      .setStages(stageBuffer.toArray[PipelineStage])

    effectiveAdaptation match {
      case _: SchemaAdaptation.IgnoreIrrelevant =>
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

    require(
      warehouse.isEmpty,
      "Cyclic pipeline stage dependency:\n" + warehouse.values.map(_.stage).mkString("\n")
    )

    result
  }

  def build(
      compaction: PathCompaction = DFD.DEFAULT_COMPACTION,
      fieldsEvidence: Array[StructField] = null, //set this to make pipeline adaptive to df being transformed.
      schemaEvidence: StructType = null, //set this to make pipeline adaptive to df being transformed.
      dfEvidence: DataFrame = null, //set this to make pipeline adaptive to df being transformed.
      adaptation: SchemaAdaptation = DFD.DEFAULT_SCHEMA_ADAPTATION
  ): Pipeline = {

    buildStagesImpl[PipelineStage](
      compaction,
      Option(fieldsEvidence)
        .orElse {
          Option(schemaEvidence).map(_.fields)
        }
        .orElse {
          Option(dfEvidence).map(_.schema.fields)
        },
      adaptation
    )
  }

  def buildModel(
      compaction: PathCompaction = DFD.DEFAULT_COMPACTION,
      fieldsEvidence: Array[StructField] = null, //set this to make pipeline adaptive to df being transformed.
      schemaEvidence: StructType = null, //set this to make pipeline adaptive to df being transformed.
      dfEvidence: DataFrame = null, //set this to make pipeline adaptive to df being transformed.
      adaptation: SchemaAdaptation = DFD.DEFAULT_SCHEMA_ADAPTATION
  ): PipelineModel = {

    coll.foreach {
      case (_, v: Step) => require(v.stage.stage.isInstanceOf[Transformer])
      case _            =>
    }

    val pipeline = buildStagesImpl[Transformer](
      compaction,
      Option(fieldsEvidence)
        .orElse {
          Option(schemaEvidence).map(_.fields)
        }
        .orElse {
          Option(dfEvidence).map(_.schema.fields)
        },
      adaptation
    )

    new PipelineModel(pipeline.uid, pipeline.getStages.map(_.asInstanceOf[Transformer]))
      .setParent(pipeline)
  }

  // preemptive buildStage with type safety check, always fail fast
  // use to validate in FlowComponent Constructor and fail early.
  // stateless, replicate self before applying propagateCols, stateful changes are discarded.
  protected def validateOnSchema(fieldsEvidence: Array[StructField]): Unit = {
    this
      .replicate()
      .buildStagesImpl[PipelineStage](
        DFD.COMPACTION_FOR_TYPECHECK,
        fieldsEvidenceOpt = Some(fieldsEvidence),
        adaptation = DFD.SCHEMA_ADAPTATION_FOR_TYPECHECK
      )
  }

  protected def validateOnSources(): Unit = {
    val fields: List[Set[StructField]] = this.sourceColl
      .filter(_._2.dataTypes.nonEmpty)
      .values
      .map { source =>
        source.dataTypes.map(t => StructField(source.name, t))
      }
      .toList

    val cartesian: Set[List[StructField]] = DSLUtils.cartesianProductSet(fields)
    val schemas = cartesian.map(v => new StructType(v.toArray))
    schemas.foreach { schema =>
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
    tails
      .map { tail =>
        val prettyTail = StepVisualWrapper(tail, showID, showInputs, showOutput, showPrefix)
        val treeNode = ForwardNode(prettyTail)
        treeNode.treeString(verbose = false)
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
    heads
      .map { head =>
        val prettyHead = StepVisualWrapper(head, showID, showInputs, showOutput, showPrefix)
        val treeNode = BackwardNode(prettyHead)
        treeNode.treeString(verbose = false)
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
    mirrorImgs.find(_._1 == char).map(_._2).getOrElse {
      mirrorImgs.find(_._2 == char).map(_._1).getOrElse {
        char
      }
    }
  }

  protected final val layoutPrefs = LayoutPrefsImpl(unicode = true, explicitAsciiBends = false)

  def showASCIIArt(
      showID: Boolean = true,
      showInputs: Boolean = true,
      showOutput: Boolean = true,
      showPrefix: Boolean = true,
      forward: Boolean = true
  ): String = {

    val prettyColl = coll.mapValues { v =>
      StepVisualWrapper(v, showID, showInputs, showOutput, showPrefix)
    }

    val vertices: Set[StepVisualWrapper] = prettyColl.values.toSet
    val edges: List[(StepVisualWrapper, StepVisualWrapper)] = prettyColl.values.toList.flatMap { v =>
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
      compactionOpt: Option[PathCompaction] = Some(DFD.DEFAULT_COMPACTION)
  ): String = {
    compactionOpt.foreach(this.propagateCols)

    if (!asciiArt) {
      if (forward) {

        "\\ left >\n" + showForwardTree(leftTails, showID, showInputs, showOutput, showPrefix) +
          "/ right <\n" + showForwardTree(rightTails, showID, showInputs, showOutput, showPrefix)
      } else {

        showBackwardTree(this.heads, showID, showInputs, showOutput, showPrefix)
      }
    } else {
      showASCIIArt(showID, showInputs, showOutput, showPrefix, forward)
    }
  }
}
