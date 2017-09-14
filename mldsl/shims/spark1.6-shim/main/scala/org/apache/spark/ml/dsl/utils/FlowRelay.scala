package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.dsl._
import org.apache.spark.ml.dsl.utils.messaging.{MessageRelay, MessageRepr}
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.sql.utils.DataTypeRelay
import org.apache.spark.util.Utils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.implicitConversions
import scala.util.Try

object FlowRelay extends MessageRelay[Flow] {

  override def toM(flow: Flow): M = {

    val steps: Seq[Step] = flow.coll.values.collect {
      case st: Step => st
    }.toSeq

    val leftWrappers = flow.leftTails.map(SimpleStepWrapper)
    val leftTrees = leftWrappers.map(flow.ForwardNode)

    val rightWrappers = flow.rightTails.map(SimpleStepWrapper)
    val rightTrees = rightWrappers.map(flow.ForwardNode)

    this.M(
      Declaration(
        steps.map(StepRelay.toM)
      ),
      Seq(
        GraphRepr(
          leftTrees.map(StepTreeNodeRelay.toM),
          `@direction` = Some(FORWARD_LEFT)
        ),
        GraphRepr(
          rightTrees.map(StepTreeNodeRelay.toM),
          `@direction` = Some(FORWARD_RIGHT)
        )
      ),
      HeadIDs(flow.headIDs)
    )
  }

  def FORWARD_RIGHT: String = "forwardRight"
  def FORWARD_LEFT: String = "forwardLeft"


  case class M(
                declarations: Declaration,
                flowLines: Seq[GraphRepr],
                headIDs: HeadIDs
              ) extends MessageRepr[Flow]{

    implicit def stepsToView(steps: StepMap[String, StepLike]): StepMapView = new StepMapView(steps)

    override def toObject: Flow = {

      val steps = declarations.stage.map(_.toObject)
      var buffer: StepMap[String, StepLike] = StepMap(steps.map(v => v.id -> v): _*)

      def treeNodeReprToLink(repr: StepTreeNodeRelay.M): Unit = {
        if (! buffer.contains(repr.id)) {
          buffer = buffer.updated(repr.id, Source(repr.id, repr.dataTypes.map(_.toObject)))
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

      Flow(
        buffer,
        leftTailIDs = leftTailIDs,
        rightTailIDs = rightTailIDs,
        headIDs = headIDs.headID
      )
    }
  }

  case class Declaration(
                          stage: Seq[StepRelay.M]
                        )

  case class GraphRepr(
                        flowLine: Seq[StepTreeNodeRelay.M],
                        `@direction`: Option[String] = None
                      )

  case class HeadIDs (
                       headID: Seq[String]
                     )
}

object StepRelay extends MessageRelay[Step] {

  val paramMap: Option[JValue]  = None

  override def toM(v: Step): M = {
    import org.json4s.JsonDSL._
    import v._

    val instance = stage.stage
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams: JValue = paramMap.getOrElse(
      render(
        params.map {
          case ParamPair(p, vv) =>
            p.name -> parse(p.jsonEncode(vv))
        }.toList
      )
    )

    M(
      id,
      stage.name,
      stage.tags,
      stage.outputColOverride,
      instance.getClass.getCanonicalName,
      Some(instance.uid),
      params = Some(jsonParams)
    )
  }


  case class M(
                id: String,
                name: String,
                tag: Set[String],
                forceOutput: Option[String],
                implementation: String,
                uid: Option[String] = None,
                params: Option[JValue] = None
              ) extends MessageRepr[Step] {

    override lazy val toObject: Step = {

      val cls = Utils.classForName(implementation)
      val instance = cls.getConstructor(classOf[String]).newInstance(uid.toSeq: _*).asInstanceOf[PipelineStage]
      getAndSetParams(instance, params.getOrElse(JNull))

      val stage = NamedStage(
        instance,
        name,
        tag,
        forceOutput,
        id
      )

      Step(stage)
    }

    //TODO: can we merge this into MessageRelay?
    def getAndSetParams(instance: Params, params: JValue): Unit = {
      implicit val format = Xml.defaultFormats
      params match {
        case JObject(pairs) =>
          pairs.foreach { case (paramName, jsonValue) =>
            val param = instance.getParam(paramName)
            val valueTry = Try {
              param.jsonDecode(compact(render(jsonValue)))
            }
              .orElse{Try{
                param.jsonDecode(compact(render(JArray(List(jsonValue)))))
              }}

            val value = jsonValue match {
              case js: JString =>
                valueTry
                  .orElse{Try{
                    param.jsonDecode(compact(render(JInt(js.values.toLong))))
                  }}
                  .orElse{Try{
                    param.jsonDecode(compact(render(JDouble(js.values.toDouble))))
                  }}
                  .orElse{Try{
                    param.jsonDecode(compact(render(JDecimal(js.values.toDouble))))
                  }}
                  .orElse{Try{
                    param.jsonDecode(compact(render(JBool(js.values.toBoolean))))
                  }}
                  .get
              case _ =>
                valueTry.get
            }

            instance.set(param, value)
          }
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot recognize JSON metadata:\n ${pretty(params)}.")
      }
    }
  }
}

object StepTreeNodeRelay extends MessageRelay[StepTreeNode[_]] {

  override def toM(v: StepTreeNode[_]): M = {
    val base = v.self match {
      case source: Source =>
        M(
          source.id,
          dataTypes = source.dataTypes
            .map(DataTypeRelay.toM)
        )
      case _ =>
        M(v.self.id)
    }
    base.copy(
      stage = v.children.map(this.toM)
    )
  }

  case class M(
                id: String,
                dataTypes: Set[DataTypeRelay.M] = Set.empty,
                stage: Seq[M] = Nil
              ) extends MessageRepr[StepTreeNode[_]] {
    override def toObject: StepTreeNode[_] = ???
  }
}

