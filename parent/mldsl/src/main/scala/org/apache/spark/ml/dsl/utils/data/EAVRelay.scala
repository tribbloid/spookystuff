package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.utils.TreeThrowable
import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.ml.dsl.utils.messaging.{CodecRegistry, MessageRelay, RelayIR}
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.json4s
import org.json4s.JsonAST.{JObject, JString, JValue}

import java.lang.reflect.{InvocationTargetException, Method}
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

trait EAVRelay[I <: EAV] extends MessageRelay[I] with EAVBuilder[I] {

  final override type Impl = I
  final override def Impl: EAVRelay[I] = this

  override def getRootTag(protoOpt: Option[I], messageOpt: Option[Map[String, JValue]]): String = "root"

  private val jvBlacklist: Set[JValue] = Set(
    JObject()
  )
  def assertWellFormed(jv: JValue): JValue = {
    assert(!jvBlacklist.contains(jv))
    jv
  }

  // TODO: too much boilerplates! should use the overriding pattern in:
  // https://stackoverflow.com/questions/55801443/in-scala-how-can-an-inner-case-class-consistently-override-a-method
  def fromCore(v: EAV.Impl): I

  final type M = Map[String, JValue]
  override def messageMF: Manifest[Map[String, JValue]] = implicitly[Manifest[M]]

  override def toMessage_>>(md: I): M = {
    val result: Seq[(String, json4s.JValue)] = md.asMap.toSeq
      .map {
        case (k, v) =>
          val ir = RelayIR
            .Value(v)
            .depthFirstTransform(
              onValue = { elem: md.VV =>
                TreeThrowable
                  .|||^[JValue](
                    Seq(
                      { () =>
                        val codec = CodecRegistry.Default.findCodecOrDefault[Any](v)
                        assertWellFormed(codec.toWriter_>>(elem).toJValue)
                      },
                      { () =>
                        JString(elem.toString)
                      }
                    )
                  )
                  .get
              }
            )

          k -> ir.toJValue
      }

    ListMap(result: _*)
  }

  override def toProto_<<(m: M, rootTag: String): I = {
    val map = m.toSeq
      .map {
        case (k, jv) =>
          val parsed = RelayIR.Codec().fromJValue(jv)
//
//          RelayIR(jv).map(
//            fn = identity,
//            preproc = {
//              case v: JValue => v.values
//              case v @ _     => v
//            }
//          )
          k -> parsed.self
      }
    fromUntypedTuples(map: _*)
  }

  case class ReflectionParser[TT: ClassTag]() {

    @transient lazy val clazz: Class[_] = implicitly[ClassTag[TT]].runtimeClass

    @transient lazy val validGetters: Array[(String, Method)] = {

      val methods = clazz.getMethods

      val _methods = methods.filter { m =>
        (m.getParameterTypes.length == 0) &&
        DSLUtils.isSerializable(m.getReturnType)
      }
//      val _methods = methods.filter { m =>
//        m.getParameterTypes.length == 0
//      }
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

    def apply(obj: TT): I = {
      val kvs: Seq[(String, Any)] = validGetters.flatMap { tuple =>
        try {
          tuple._2.setAccessible(true)
          Some(tuple._1 -> tuple._2.invoke(obj).asInstanceOf[Any])
        } catch {
          case e: InvocationTargetException =>
            None
        }
      }.toList
      EAVRelay.this.fromUntypedTuples(kvs: _*)
    }
  }

  object RuntimeReflectionParser {
    def apply[TT](obj: TT): I = {
      val scalaType = ScalaType.FromClass[TT](obj.getClass.asInstanceOf[Class[TT]])
      implicit val classTag: ClassTag[TT] = scalaType.asClassTag

      ReflectionParser[TT]().apply(obj)
    }
  }
}
