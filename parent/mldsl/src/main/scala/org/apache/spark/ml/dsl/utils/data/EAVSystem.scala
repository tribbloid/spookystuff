package org.apache.spark.ml.dsl.utils.data

import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.ml.dsl.utils.messaging.Encoder.HasEncoder
import org.apache.spark.ml.dsl.utils.messaging.{Relay, TreeIR}

import java.lang.reflect.{InvocationTargetException, Method}
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

trait EAVSystem {

  trait ThisEAV extends EAV {

    final override def system = EAVSystem.this
  }

  object ThisEAV extends HasEncoder[_EAV] {

    implicit def relay(
        implicit
        cc: ClassTag[Bound]
    ): Relay[_EAV] = _Relay()
  }

  type _EAV <: ThisEAV
  val _EAV: collection.Map[String, Bound] => _EAV

  final type Bound = _EAV#Bound

  lazy val empty: _EAV = From.tuple()

  class From[V >: Bound](
      private val constructor: collection.Map[String, V] => _EAV
  ) {

    final def iterableInternal(kvs: Iterable[(String, V)]): _EAV = {

      constructor(ListMap[String, V](kvs.toSeq: _*))
    }

    final def iterable(kvs: Iterable[Tuple2[_, V]]): _EAV = {
      val _kvs = kvs.map {
        case (k: _EAV#Attr[_], v) =>
          k.primaryName -> v
        case (k: String, v) =>
          k -> v
        case (k, v) =>
          throw new UnsupportedOperationException(s"unsupported key type for $k")
      }

      iterableInternal(_kvs)
    }

    final def tuple(kvs: Tuple2[_, V]*): _EAV = {
      iterable(kvs)
    }

    final def apply(kvs: Magnets.AttrValueMag[_ <: V]*): _EAV = {
      val _kvs = kvs.flatMap { m =>
        m.vOpt.map { v =>
          m.k -> v
        }
      }

      tuple(_kvs: _*)
    }
  }

  case object From extends From[Bound](_EAV) {

    case class FromAny()(
        implicit
        clazz: ClassTag[Bound]
    ) extends From[Any](
          { map =>
            val _map = map.collect {
              case (k, v: Bound) => k -> v
              case (k, null)     => k -> null.asInstanceOf[Bound]
            }
            _EAV(_map)
          }
        )

    def any(
        implicit
        clazz: ClassTag[Bound]
    ): FromAny = FromAny()(clazz)
  }

  case class _Relay()(
      implicit
      cc: ClassTag[Bound]
  ) extends Relay[_EAV] {

    final type Msg = Any

    override def toMessage_>>(eav: _EAV): Msg = {
      val raw: TreeIR.Leaf[_EAV] = TreeIR.Leaf(eav)

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

    override def toProto_<<(m: Msg, rootTag: String): _EAV = {

      val relay = TreeIR._Relay[Any]()
      val ir: TreeIR[Any] = relay.toProto_<<(m, rootTag)

      val collected = ir.depthFirstTransform
        .up[Any, TreeIR.Leaf[Any]] {
          case struct: TreeIR.Struct[_] =>
            val map = struct.toMessage_>>
            val eav = From.any.iterableInternal(map)
            TreeIR.Leaf(eav)
          case ll: TreeIR.Leaf[_] =>
            ll.asInstanceOf[TreeIR.Leaf[Any]]
        }
        .execute

      collected.repr.asInstanceOf[_EAV]
    }

  }

  case class ReflectionParser[TT]()(
      implicit
      tcc: ClassTag[TT],
      bcc: ClassTag[Bound]
  ) {

    @transient lazy val clazz: Class[_] = tcc.runtimeClass

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

    def apply(obj: TT): _EAV = {
      val kvs: Seq[(String, Any)] = validGetters.flatMap { tuple =>
        try {
          tuple._2.setAccessible(true)
          Some(tuple._1 -> tuple._2.invoke(obj).asInstanceOf[Any])
        } catch {
          case e: InvocationTargetException =>
            None
        }
      }.toList
      EAVSystem.this.From.any.iterableInternal(kvs)
    }
  }

  implicit def sys: EAVSystem.Aux[_EAV] = EAVSystem.this

  def relay(
      implicit
      cc: ClassTag[Bound]
  ): _Relay = _Relay()

  implicit def toRelay(v: this.type)(
      implicit
      cc: ClassTag[Bound]
  ): _Relay = relay
}

object EAVSystem {

  type Aux[T] = EAVSystem { type _EAV = T }

  object NoAttr extends EAVSystem {

    case class _EAV(internal: collection.Map[String, Any]) extends ThisEAV {
      override type Bound = Any
    }
  }
  type NoAttr = NoAttr._EAV

}
