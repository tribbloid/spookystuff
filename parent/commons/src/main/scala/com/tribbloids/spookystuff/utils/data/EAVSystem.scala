package com.tribbloids.spookystuff.utils.data

import com.tribbloids.spookystuff.relay.io.Encoder.HasEncoder
import com.tribbloids.spookystuff.relay.{Relay, TreeIR}
import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.lang.reflect.{InvocationTargetException, Method}
import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag

trait EAVSystem {

  trait EAV extends EAVLike {

    final override def system: EAVSystem = EAVSystem.this

    @transient lazy val canonical: ^ = ^(Map(KVs.defined: _*))
  }

  object EAV extends HasEncoder[^] {

    // TODO: don't know how to do this, causing problem in generating KVs
    trait CaseInsensitive extends EAV {

      override protected def getLookup: Map[String, Any] = CaseInsensitiveMap(super.getLookup)
    }

    implicit def relay: Relay[^] = _Relay()

    /**
      * will determine ordering by the following evidences, in descending precedence:
      *   - values of the defined attributes, in the order of definition
      */
    private lazy val _defaultOrdering: Ordering[_ <: EAV] = {
      import Ordering.Implicits._

      Ordering.by { v: EAV =>
        v.sortEvidence
      }
    }

    implicit def defaultOrdering[T <: EAV]: Ordering[T] = {
      _defaultOrdering.asInstanceOf[Ordering[T]]
    }
  }

  type ^ <: EAV
  val ^ : collection.Map[String, Any] => ^

  lazy val empty: `^` = From.tuple()

  class From[V >: Any](
      private val constructor: collection.Map[String, V] => ^
  ) {

    final def iterableInternal(kvs: Iterable[(String, V)]): ^ = {

      constructor(ListMap[String, V](kvs.toSeq: _*))
    }

    final def iterable(kvs: Iterable[Tuple2[_, V]]): ^ = {
      val _kvs = kvs.map {
        case (k: `^` #Attr[_], v) =>
          k.name -> v
        case (k: String, v) =>
          k -> v
        case (k, _) =>
          throw new UnsupportedOperationException(s"unsupported key type for $k")
      }

      iterableInternal(_kvs)
    }

    final def tuple(kvs: Tuple2[_, V]*): ^ = {
      iterable(kvs)
    }

    final def apply(kvs: Magnets.AttrValueMag[_ <: V]*): ^ = {
      val _kvs = kvs.flatMap { m =>
        m.vOpt.map { v =>
          m.k -> v
        }
      }

      tuple(_kvs: _*)
    }
  }

  case object From extends From[Any](^) {
    // TODO: cleanup, useless
    case class FromAny()
        extends From[Any](
          { map =>
            val _map = map.collect {
              case (k, v: Any) => k -> v
              case (k, null)   => k -> null.asInstanceOf[Any]
            }
            ^(_map)
          }
        )

    def any: FromAny = FromAny()
  }

  case class _Relay() extends Relay[^] {

    final type IR_>> = TreeIR[Any]
    final type IR_<< = TreeIR.Leaf[Any]

    override def toMessage_>>(eav: ^): IR_>> = {
      val raw: TreeIR.Leaf[^] = TreeIR.leaf(eav)

      val expanded = raw.DepthFirstTransform
        .down[Any] {
          case ll @ TreeIR.Leaf(v: EAVLike, _) =>
            val sub = v.KVs.raw
            val subNodes: Seq[(String, TreeIR.Leaf[Any])] = sub.map {
              case (kk, vv) =>
                kk -> TreeIR.leaf[Any](vv)
            }

            TreeIR.Builder(Some(ll.rootTag)).map(subNodes: _*)
          case others @ _ =>
            others
        }
        .execute

      expanded
    }

    override def toProto_<<(m: IR_<<): ^ = {

      val canonical = {
        m.explode.explodeStringMap()
      }

      val folded = canonical.DepthFirstTransform
        .up[Any, TreeIR.Leaf[Any]] {
          case struct: TreeIR.MapTree[_, _] =>
            val map = struct.body
            val stringMap = map.map {
              case (k, v) =>
                ("" + k) -> v
            }
            val eav = From.any.iterableInternal(stringMap)
            TreeIR.Builder(Some(struct.rootTag)).leaf(eav)
          case ll: TreeIR.ListTree[_] =>
            TreeIR.Builder(Some(ll.rootTag)).leaf(ll.body)
          case ll: TreeIR.Leaf[_] =>
            ll.upcast[Any]
        }
        .execute

      folded.body.asInstanceOf[^]
    }
  }

  case class ReflectionParser[TT]()(
      implicit
      tcc: ClassTag[TT]
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

    def apply(obj: TT): ^ = {
      val kvs: Seq[(String, Any)] = validGetters.flatMap { tuple =>
        try {
          tuple._2.setAccessible(true)
          Some(tuple._1 -> tuple._2.invoke(obj).asInstanceOf[Any])
        } catch {
          case _: InvocationTargetException =>
            None
        }
      }.toList
      EAVSystem.this.From.any.iterableInternal(kvs)
    }
  }

  implicit def sys: EAVSystem.Aux[^] = EAVSystem.this

  def relay: _Relay = _Relay()

  implicit def toRelay(v: this.type): _Relay = relay
}

object EAVSystem {

  type Aux[T] = EAVSystem { type ^ = T }

  object NoAttr extends EAVSystem {

    case class ^(internal: collection.Map[String, Any]) extends EAV {}
  }
  type NoAttr = NoAttr.^

}
