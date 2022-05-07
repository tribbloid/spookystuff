package org.apache.spark.ml.dsl.utils.data

import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.ml.dsl.utils.messaging.{MessageRelay, TreeIR}
import org.apache.spark.ml.dsl.utils.refl.ScalaType

import java.lang.reflect.{InvocationTargetException, Method}
import scala.reflect.ClassTag

trait EAVRelay[I <: EAV] extends MessageRelay[I] with EAVBuilder[I] {

  final override type Impl = I
  final override def Impl: EAVRelay[I] = this

  // TODO: too much boilerplates! should use the overriding pattern in:
  // https://stackoverflow.com/questions/55801443/in-scala-how-can-an-inner-case-class-consistently-override-a-method
  def fromCore(v: EAV.Impl): I

  final type M = Any
  override def messageMF: Manifest[M] = implicitly[Manifest[M]]

  override def toMessage_>>(eav: I): M = {
    val raw: TreeIR.Leaf[I] = TreeIR.Leaf(eav)

    val expanded = raw.depthFirstTransform
      .down[Any] {
        case TreeIR.Leaf(v: EAV) =>
          val sub = v.asMap.toSeq
          val subNodes: Seq[(String, TreeIR.Leaf[Any])] = sub.map {
            case (kk, vv) =>
              kk -> TreeIR.Leaf[Any](vv)
          }

          TreeIR.Struct.Builder().fromKVs(subNodes: _*)
        case others @ _ =>
          others
      }
      .execute

    expanded.toMessage_>>
  }

  override def toProto_<<(m: M, rootTag: String): I = {

    val relay = TreeIR._Relay[Any]()
    val ir: TreeIR[Any] = relay.toProto_<<(m, rootTag)

    val collected = ir.depthFirstTransform
      .up[Any, TreeIR.Leaf[Any]] {
        case struct: TreeIR.Struct[_] =>
          val map = struct.toMessage_>>
          TreeIR.Leaf(fromUntypedTuples(map.toSeq: _*))
        case ll: TreeIR.Leaf[_] =>
          ll.asInstanceOf[TreeIR.Leaf[Any]]
      }
      .execute

    collected.repr.asInstanceOf[I]
  }

  case class ReflectionParser[TT: ClassTag]() {

    @transient lazy val clazz: Class[_] = implicitly[ClassTag[TT]].runtimeClass

    @transient lazy val validGetters: Array[(String, Method)] = {

      val methods = clazz.getMethods

      val _methods = methods.filter { m =>
        (m.getParameterTypes.length == 0) &&
        DSLUtils.isSerializable(m.getReturnType)
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
