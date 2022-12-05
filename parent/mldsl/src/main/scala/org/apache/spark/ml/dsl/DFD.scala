package org.apache.spark.ml.dsl

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI, Relay}
import org.apache.spark.sql.types.StructField

import scala.collection.mutable
import scala.language.implicitConversions

object DFD extends Relay.<<[DFD] {

  final val DEFAULT_COMPACTION: PathCompaction = Compactions.PruneDownPath
  final val DEFAULT_SCHEMA_ADAPTATION: SchemaAdaptation = SchemaAdaptation.FailFast

  final val COMPACTION_FOR_TYPECHECK: PathCompaction = Compactions.DoNotCompact
  final val SCHEMA_ADAPTATION_FOR_TYPECHECK: SchemaAdaptation = SchemaAdaptation.IgnoreIrrelevant_ValidateSchema

  def apply(v: PipelineStage): Step = v
  def apply(tuple: (PipelineStage, Any)): Step = tuple
  def apply(s: Symbol): Source = s
  def apply(s: StructField): Source = s

  override def toMessage_>>(flow: DFD): Msg = {

    flow.propagateCols(DFD.DEFAULT_COMPACTION)

    val steps: Seq[Step] = flow.coll.values.collect {
      case st: Step => st
    }.toSeq

    val leftWrappers = flow.leftTails.map(SimpleStepWrapper)
    val leftTrees = leftWrappers.map(flow.ForwardNode)

    val rightWrappers = flow.rightTails.map(SimpleStepWrapper)
    val rightTrees = rightWrappers.map(flow.ForwardNode)

    this.Msg(
      Declaration(
        steps.map(Step.toMessage_>>)
      ),
      Seq(
        GraphRepr(
          leftTrees.map(StepTreeNode.toMessage_>>),
          `@direction` = Some(FORWARD_LEFT)
        ),
        GraphRepr(
          rightTrees.map(StepTreeNode.toMessage_>>),
          `@direction` = Some(FORWARD_RIGHT)
        )
      ),
      HeadIDs(flow.headIDs)
    )
  }

  def FORWARD_RIGHT: String = "forwardRight"
  def FORWARD_LEFT: String = "forwardLeft"

  case class Msg(
      declarations: Declaration,
      flowLines: Seq[GraphRepr],
      headIDs: HeadIDs
  ) extends MessageAPI.<< {

    implicit def stepsToView(steps: StepMap[String, StepLike]): StepMapView = new StepMapView(steps)

    override def toProto_<< : DFD = {

      val steps = declarations.stage.map(_.toProto_<<)
      var buffer: StepMap[String, StepLike] = StepMap(steps.map(v => v.id -> v): _*)

      def treeNodeReprToLink(repr: StepTreeNode.Msg): Unit = {
        if (!buffer.contains(repr.id)) {
          buffer = buffer.updated(repr.id, Source(repr.id, repr.dataTypes.map(_.toProto_<<)))
        }
        val children = repr.stage
        buffer = buffer.connectAll(Seq(repr.id), children.map(_.id))
        children.foreach(treeNodeReprToLink)
      }

      for (
        graph <- flowLines;
        tree <- graph.flowLine
      ) {
        treeNodeReprToLink(tree)
      }

      val leftTailIDs = flowLines.filter(_.`@direction`.exists(_ == FORWARD_LEFT)).flatMap(_.flowLine.map(_.id))
      val rightTailIDs = flowLines.filter(_.`@direction`.exists(_ == FORWARD_RIGHT)).flatMap(_.flowLine.map(_.id))

      DFD(
        buffer,
        leftTailIDs = leftTailIDs,
        rightTailIDs = rightTailIDs,
        headIDs = headIDs.headID
      )
    }
  }

  case class Declaration(
      stage: Seq[Step.Msg]
  )

  case class GraphRepr(
      flowLine: Seq[StepTreeNode.Msg],
      `@direction`: Option[String] = None
  )

  case class HeadIDs(
      headID: Seq[String]
  )
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

}

//TODO: should I be using decorator/mixin?
/**
  * End result of the DSL that can be converted to a Spark ML pipeline
  * @param coll
  * @param leftTailIDs
  * @param rightTailIDs
  * @param headIDs
  * @param fromIDsOpt
  */
case class DFD(
    coll: StepMap[String, StepLike],
    leftTailIDs: Seq[String],
    rightTailIDs: Seq[String],
    headIDs: Seq[String],
    fromIDsOpt: Option[Seq[String]] = None // overrridden by using "from" function
) extends DFDComponent {

  override def fromIDs: Seq[String] = fromIDsOpt.getOrElse(headIDs)

  lazy val stages: Array[PipelineStage] = coll.values
    .collect {
      case st: Step =>
        st.stage.stage
    }
    .toArray
    .distinct

  def from(name: String): DFD = {
    val newFromIDs = coll.values.filter(_.name == name).map(_.id).toSeq
    this.copy(
      fromIDsOpt = Some(newFromIDs)
    )
  }
  def :>-(name: String): DFD = from(name)

  def and(name: String): DFD = {
    val newFromIDs = coll.values.filter(_.name == name).map(_.id).toSeq
    this.copy(
      fromIDsOpt = Some(this.fromIDs ++ newFromIDs)
    )
  }
  def :&&(name: String): DFD = and(name)

  def replicate(suffix: String = ""): DFD = {

    val idConversion = mutable.Map[String, String]()

    val newSteps: StepMap[String, StepLike] = replicateColl(suffix = suffix, idConversion = idConversion)

    new DFD(
      newSteps,
      leftTailIDs.map(idConversion),
      rightTailIDs.map(idConversion),
      headIDs.map(idConversion),
      this.fromIDsOpt
    )
  }
}
