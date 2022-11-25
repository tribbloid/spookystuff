package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.tree.TreeView

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

trait RelayIR[+V] extends Product with ProtoAPI {

  import RelayIR._

  def children: Seq[RelayIR[V]]

  def depthFirstTransform[V1 >: V, V2](
      onValue: V1 => V2,
      down: RelayIR[V1] => RelayIR[V1] = identity[RelayIR[V1]] _,
      up: RelayIR[V2] => RelayIR[V2] = identity[RelayIR[V2]] _
  ): RelayIR[V2] = {

    val afterDown = down(this)

    val transformed = afterDown match {
      case sub: Obj[V1] =>
        Obj(
          sub.self.map {
            case (k, v) =>
              k -> v.depthFirstTransform(onValue, down, up)
          }
        )
      case Value(v) =>
        Value(onValue(v))
    }

    val afterUp = up(transformed)

    afterUp
  }

  def pathToValueMap: Map[Seq[String], V]

  def treeView: _TreeView = _TreeView(this)
}

object RelayIR {

  implicit def fromV[V](v: V): Value[V] = Value(v)

  implicit def fromKV[K, V](kv: (K, V)): (K, Value[V]) = kv._1 -> Value(kv._2)

  val _CLASS_NAME: String = classOf[RelayIR[_]].getSimpleName

  def buildFromKVs[V](kvs: (String, RelayIR[V])*): Obj[V] = Obj(ListMap(kvs: _*))

  case class Codec[V](
  ) extends MessageRelay[RelayIR[V]] {

    type M = Any
    override val messageMF: Manifest[Any] = Manifest.Any

    override def toProto_<<(v: Any, rootTag: String): RelayIR[V] = {

      v match {
        case vs: collection.Map[String, Any] =>
          val mapped = ListMap(vs.toSeq: _*).map {
            case (kk, vv) =>
              kk -> toProto_<<(vv, rootTag)
          }
          Obj(mapped)

        case vv: V =>
          Value(vv)
        case _ =>
          throw new UnsupportedOperationException(
            s"JVM class ${v.getClass} cannot be mapped to ${_CLASS_NAME}"
          )
      }
    }
  }

  case class _TreeView(self: RelayIR[_]) extends TreeView.Immutable[_TreeView] {
    override lazy val nodeName: String = self.getClass.getSimpleName

    override lazy val children: Seq[_TreeView] = self.children.map(_TreeView)

    override def stringArgs: Iterator[Any] =
      if (children.isEmpty) self.productIterator
      else Iterator.empty
  }

  case class Value[+V](self: V) extends RelayIR[V] {
    override def toMessage_>> : Any = self

    override def children: Seq[RelayIR[V]] = Nil

    override lazy val pathToValueMap: ListMap[Seq[String], V] = ListMap(Nil -> self)
  }

  case class Obj[+V](self: ListMap[String, RelayIR[V]]) extends RelayIR[V] {
    override def toMessage_>> : Any = self.mapValues { v =>
      v.toMessage_>>
    }

    override lazy val children: Seq[RelayIR[V]] = self.values.toSeq

    override lazy val pathToValueMap: ListMap[Seq[String], V] = {
      self.flatMap {
        case (k, v) =>
          v.pathToValueMap.map {
            case (kk, v) =>
              (Seq(k) ++ kk) -> v
          }
      }
    }
  }
}
