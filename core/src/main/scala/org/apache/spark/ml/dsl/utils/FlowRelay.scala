package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.dsl._
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.implicitConversions
import scala.util.Try

object FlowRelay extends StructRelay[Flow] {

  override def toRepr(flow: Flow): Repr = {

    val steps: Seq[Step] = flow.coll.values.collect {
      case st: Step => st
    }.toSeq

    val leftWrappers = flow.leftTails.map(SimpleStepWrapper)
    val leftTrees = leftWrappers.map(flow.ForwardNode)


    val rightWrappers = flow.rightTails.map(SimpleStepWrapper)
    val rightTrees = rightWrappers.map(flow.ForwardNode)

    this.Repr(
      Declaration(
        steps.map(_.toRepr)
      ),
      Seq(
        GraphRepr(
          leftTrees.map(_.toRepr),
          `@direction` = Some(FORWARD_LEFT)
        ),
        GraphRepr(
          rightTrees.map(_.toRepr),
          `@direction` = Some(FORWARD_RIGHT)
        )
      ),
      HeadIDs(flow.headIDs)
    )
  }

  def FORWARD_RIGHT: String = "forwardRight"
  def FORWARD_LEFT: String = "forwardLeft"


  case class Repr(
                   declarations: Declaration,
                   flowLines: Seq[GraphRepr],
                   headIDs: HeadIDs
                 ) extends StructRepr[Flow]{

//    override def XML_ROOT = "flow" //TODO: cleanup?

    implicit def stepsToView(steps: StepMap[String, StepLike]): StepMapView = new StepMapView(steps)

    override def toSelf: Flow = {

      val steps = declarations.stage.map(_.toSelf)
      var buffer: StepMap[String, StepLike] = StepMap(steps.map(v => v.id -> v): _*)

      def treeNodeReprToLink(repr: StepTreeNodeRelay.Repr): Unit = {
        if (! buffer.contains(repr.id)) {
          buffer = buffer.updated(repr.id, Source(repr.id, repr.dataTypes.map(_.toSelf)))
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
                          stage: Seq[StepRelay.Repr]
                        )

  case class GraphRepr(
                        flowLine: Seq[StepTreeNodeRelay.Repr],
                        `@direction`: Option[String] = None
                      )

  case class HeadIDs (
                       headID: Seq[String]
                     )
}

object StepRelay extends StructRelay[Step] {

  val paramMap: Option[JValue]  = None

  override def toRepr(v: Step): Repr = {
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

    Repr(
      id,
      stage.name,
      stage.tags,
      stage.outputColOverride,
      instance.getClass.getCanonicalName,
      Some(instance.uid),
      params = Some(jsonParams)
    )
  }


  case class Repr(
                   id: String,
                   name: String,
                   tag: Set[String],
                   forceOutput: Option[String],
                   implementation: String,
                   uid: Option[String] = None,
                   params: Option[JValue] = None
                 ) extends StructRepr[Step] {

    override lazy val toSelf: Step = {

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

object StepTreeNodeRelay extends StructRelay[StepTreeNode[_]] {

  override def toRepr(v: StepTreeNode[_]): Repr = {
    val base = v.self match {
      case source: Source =>
        Repr(
          source.id,
          dataTypes = source.dataTypes
            .map(DataTypeRelay.toRepr)
        )
      case _ =>
        Repr(v.self.id)
    }
    base.copy(
      stage = v.children.map(this.toRepr)
    )
  }


  case class Repr(
                   id: String,
                   dataTypes: Set[DataTypeRelay.Repr] = Set.empty,
                   stage: Seq[Repr] = Nil
                 ) extends StructRepr[StepTreeNode[_]] {
    override def toSelf: StepTreeNode[_] = ???
  }


}

object DataTypeRelay extends StructRelay[DataType] {

  def toJsonAST(dataType: DataType): JValue = {
    ReflectionUtils.invoke(classOf[DataType], dataType, "jsonValue").asInstanceOf[JValue]
  }

  def fromJsonAST(jv: JValue): DataType = {
    ReflectionUtils.invoke(DataType.getClass, DataType, "parseDataType", classOf[JValue] -> jv).asInstanceOf[DataType]
  }


  override def toRepr(v: DataType): Repr = Repr(
    toJsonAST(v)
  )

  case class Repr (
                    dataType: JValue
                  ) extends StructRepr[DataType] {

    override def toSelf: DataType = fromJsonAST(dataType)
  }
}