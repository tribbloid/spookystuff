package org.apache.spark.ml.dsl

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.dsl.utils.messaging.{MessageAPI_<<, MessageRelay}
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{compact, parse, pretty, render}
import org.json4s.{JArray, JBool, JDecimal, JDouble, JInt, JNull, JString, JValue}

import scala.collection.mutable
import scala.util.Try

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

object Step extends MessageRelay[Step] {

  val paramMap: Option[JValue]  = None

  override def toMessage_>>(v: Step): M = {
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
              ) extends MessageAPI_<< {

    override lazy val toProto_<< : Step = {

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
//      implicit val format = Xml.defaultFormats
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