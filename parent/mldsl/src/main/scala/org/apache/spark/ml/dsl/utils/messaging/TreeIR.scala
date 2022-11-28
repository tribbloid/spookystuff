package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.tree.TreeView

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.language.implicitConversions

/**
  * IR stands for "Intermediate Representation"
  * @tparam V
  *   leaf type
  */
trait TreeIR[+V] extends Product with ProtoAPI {

  import TreeIR._

  def children: Seq[TreeIR[V]]

  def depthFirstTransform[V1 >: V, V2](
      onValue: V1 => V2,
      down: TreeIR[V1] => TreeIR[V1] = identity[TreeIR[V1]] _,
      up: TreeIR[V2] => TreeIR[V2] = identity[TreeIR[V2]] _
  ): TreeIR[V2] = {

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

object TreeIR {

  implicit def fromV[V](v: V): Value[V] = Value(v)

  implicit def fromKV[K, V](kv: (K, V)): (K, Value[V]) = kv._1 -> Value(kv._2)

  val _CLASS_NAME: String = classOf[TreeIR[_]].getSimpleName

  def fromKVs[V](kvs: (String, TreeIR[V])*): Obj[V] = Obj(ListMap(kvs: _*))

  case class Codec[V](
  ) extends MessageRelay[TreeIR[V]] {

    type M = Any
    override val messageMF: Manifest[Any] = Manifest.Any

    override def toProto_<<(v: Any, rootTag: String): TreeIR[V] = {

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

  case class _TreeView(self: TreeIR[_]) extends TreeView.Immutable[_TreeView] {
    override lazy val nodeName: String = self.getClass.getSimpleName

    override lazy val children: Seq[_TreeView] = self.children.map(_TreeView)

    override def stringArgs: Iterator[Any] =
      if (children.isEmpty) self.productIterator
      else Iterator.empty
  }

  case class Value[+V](self: V) extends TreeIR[V] {

    override def toMessage_>> : Any = self

    override def children: Seq[TreeIR[V]] = Nil

    override lazy val pathToValueMap: ListMap[Seq[String], V] = ListMap(Nil -> self)
  }

  case class Obj[+V](self: ListMap[String, TreeIR[V]]) extends TreeIR[V] {

    override def toMessage_>> : Any = {

      val view = self.mapValues { v =>
        v.toMessage_>>
      }

      view.toMap
    }

    override lazy val children: Seq[TreeIR[V]] = self.values.toSeq

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
