package org.apache.spark.ml.dsl.utils.metadata

import java.lang.reflect.{InvocationTargetException, Method}

import com.tribbloids.spookystuff.utils.TreeException
import org.apache.spark.ml.dsl.utils.FlowUtils
import org.apache.spark.ml.dsl.utils.messaging.{MessageRelay, MessageWriter, Nested, Registry}
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.json4s
import org.json4s.JsonAST.{JObject, JString, JValue}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag

trait ParamsRelay[T <: Params] extends MessageRelay[T] {

  override def getRootTag(protoOpt: Option[T], messageOpt: Option[Map[String, JValue]]): String = "root"

  private val jvBlacklist: Set[JValue] = Set(
    JObject()
  )
  def assertWellFormed(jv: JValue): JValue = {
    assert(!jvBlacklist.contains(jv))
    jv
  }

  def fromListMap(vs: ListMap[String, Any]): T

  def apply(vs: Tuple2[String, Any]*): T = fromListMap(ListMap(vs: _*))

  implicit def fromMap(map: Map[String, Any]): T = apply(map.toSeq: _*)
  implicit def toMap(v: T) = v.self

  val empty: T = apply()

  type M = Map[String, JValue]
  override def messageMF = implicitly[Manifest[M]]

  override def toMessage_>>(md: T): M = {
    val result: Seq[(String, json4s.JValue)] = md.self.toSeq
      .map {
        case (k, v) =>
          val mapped = Nested[Any](v).map[JValue] { elem: Any =>
            TreeException
              .|||^[JValue](Seq(
                { () =>
                  val codec = Registry.Default.findCodecOrDefault(v)
                  assertWellFormed(codec.toWriter_>>(elem).toJValue)
                }, { () =>
                  JString(elem.toString)
                }
              ))
              .get
          }
          k -> MessageWriter(mapped.self).toJValue
      }

    ListMap(result: _*)
  }

  override def toProto_<<(m: M, rootTag: String): T = {
    val map = m.toSeq
      .map {
        case (k, jv) =>
          val mapped = Nested[Any](jv).map(fn = identity, preproc = {
            case v: JValue => v.values
            case v @ _     => v
          })
          k -> mapped.self
        //          RecursiveTransform(jv, failFast = true)(
        //            {
        //              case v: JValue => v.values
        //              case v@ _ => v
        //            }
        //          )
      }
    apply(map: _*)
  }

  //  def ParamsParser(vs: Tuple2[MetadataLike#Param[_], Any]*) =
  //    Metadata(ListMap(vs.map { case (k, v) => k.name -> v }: _*))

  case class ReflectionParser[TT: ClassTag]() {

    @transient lazy val clazz: Class[_] = implicitly[ClassTag[TT]].runtimeClass

    @transient lazy val validGetters: Array[(String, Method)] = {

      val methods = clazz.getMethods
      val _methods = methods.filter { m =>
        (m.getParameterTypes.length == 0) &&
        FlowUtils.isSerializable(m.getReturnType)
      }
      val commonGetters = _methods
        .filter { m =>
          m.getName.startsWith("get")
        }
        .map(v => v.getName.stripPrefix("get") -> v)
      val booleanGetters = _methods
        .filter { m =>
          m.getName.startsWith("is")
        }
        .map(v => v.getName -> v)

      (commonGetters ++ booleanGetters).sortBy(_._1)
    }

    def apply(obj: TT) = {
      val kvs = validGetters.flatMap { tuple =>
        try {
          tuple._2.setAccessible(true)
          Some(tuple._1 -> tuple._2.invoke(obj).asInstanceOf[Any])
        } catch {
          case e: InvocationTargetException =>
            None
        }
      }
      ParamsRelay.this.apply(kvs: _*)
    }
  }

  @Deprecated //use ReflectionParser
  object RuntimeReflectionParser {
    def apply[TT](obj: TT) = {
      val scalaType = ScalaType.fromClass[TT](obj.getClass.asInstanceOf[Class[TT]])
      implicit val classTag: ClassTag[TT] = scalaType.asClassTag

      ReflectionParser[TT]().apply(obj)
    }
  }
}
