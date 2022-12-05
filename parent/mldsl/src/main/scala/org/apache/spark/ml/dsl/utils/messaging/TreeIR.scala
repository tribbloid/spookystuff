package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.tree.TreeView

import scala.collection.immutable.ListMap

/**
  * IR stands for "Intermediate Representation"
  * @tparam LEAF
  *   leaf type
  */
trait TreeIR[LEAF] extends Product with ProtoAPI {

  import TreeIR._

  def children: Seq[TreeIR[LEAF]]

  case class DepthFirstTransform[V1 >: LEAF, V2, RES <: TreeIR[V2]](
      val downFn: TreeIR[V1] => TreeIR[V1],
      val onLeafFn: V1 => V2,
      val upFn: TreeIR[V2] => RES
  ) {

    def execute: RES = {

      val afterDown: TreeIR[V1] = downFn(TreeIR.this.asInstanceOf[TreeIR[V1]])

      val transformed: TreeIR[V2] = afterDown match {
        case sub: Struct[V1] =>
          sub.copy(
            sub.repr.map {
              case (k, v) =>
                k -> v.DepthFirstTransform(downFn, onLeafFn, upFn).execute
            }
          )(sub.prefixOvrd)
        case Leaf(v) =>
          Leaf(onLeafFn(v))
      }

      val afterUp = upFn(transformed)

      afterUp
    }

    def down[V1N >: LEAF](fn: TreeIR[V1N] => TreeIR[V1N]): DepthFirstTransform[V1N, V1N, TreeIR[V1N]] = { // reset onLeaf & up
      DepthFirstTransform[V1N, V1N, TreeIR[V1N]](fn, identity _, identity _)
    }

    def onLeaf[V2N](fn: V1 => V2N): DepthFirstTransform[V1, V2N, TreeIR[V2N]] = { // reset up
      DepthFirstTransform(downFn, fn, identity _)
    }

    def up[V2N >: V2, RES <: TreeIR[V2N]](fn: TreeIR[V2N] => RES): DepthFirstTransform[V1, V2N, RES] = {
      DepthFirstTransform(downFn, onLeafFn, fn)
    }
  }

  def depthFirstTransform: DepthFirstTransform[LEAF, LEAF, TreeIR[LEAF]] =
    DepthFirstTransform[LEAF, LEAF, TreeIR[LEAF]](identity _, identity _, identity _)

  def pathToValueMap: Map[Seq[String], LEAF]

  def treeView: _TreeView = _TreeView(this)
}

object TreeIR {

  lazy val _CLASS_NAME: String = classOf[TreeIR[_]].getSimpleName

  case class _Relay[V]() extends Relay.>>[TreeIR[V]] {

    type Msg = Any

    override def toProto_<<(v: Any, rootTag: String): TreeIR[V] = {

      v match {
        case vs: collection.Map[String, Any] =>
          val mapped = ListMap(vs.toSeq: _*).map {
            case (kk, vv) =>
              kk -> toProto_<<(vv, rootTag)
          }
          val prefixOpt = vs match {
            case x: Product => Some(x.productPrefix)
            case _          => None
          }

          Struct(mapped)(prefixOpt)

        case vv: V =>
          Leaf(vv)
        case null =>
          Leaf(null.asInstanceOf[V])
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

  case class Leaf[LEAF](repr: LEAF) extends TreeIR[LEAF] {

    override def toMessage_>> : Any = repr

    override def children: Seq[TreeIR[LEAF]] = Nil

    override lazy val pathToValueMap: ListMap[Seq[String], LEAF] = ListMap(Nil -> repr)
  }

  case class Struct[LEAF](repr: ListMap[String, TreeIR[LEAF]])(val prefixOvrd: Option[String]) extends TreeIR[LEAF] {

    override lazy val productPrefix: String = prefixOvrd.getOrElse(
      super.productPrefix
    )

    override lazy val toMessage_>> : ProductMap = {

      val view = repr.map {
        case (k, v) =>
          k -> v.toMessage_>>
      }

      ProductMap(view, productPrefix)
    }

    override lazy val children: Seq[TreeIR[LEAF]] = repr.values.toSeq

    override lazy val pathToValueMap: ListMap[Seq[String], LEAF] = {
      repr.flatMap {
        case (k, v) =>
          v.pathToValueMap.map {
            case (kk, v) =>
              (Seq(k) ++ kk) -> v
          }
      }
    }
  }

  object Struct {

    case class Builder(prefixOpt: Option[String] = None) {

      def fromKVs[V](kvs: (String, TreeIR[_ <: V])*): Struct[V] = {
        val _kvs = kvs.toSeq.map {
          case (k, v) =>
            k -> v.asInstanceOf[TreeIR[V]]
        }
        Struct(ListMap(_kvs: _*))(prefixOpt)
      }
    }
  }

  case class ProductMap(
      self: ListMap[String, Any],
      override val productPrefix: String
  ) extends Map[String, Any] {

    override def productElement(n: Int): Any = self.toSeq(n)._2

    override def productArity: Int = self.size

    override def +[B1 >: Any](kv: (String, B1)): Map[String, B1] = this.copy(self = self + kv)

    override def get(key: String): Option[Any] = self.get(key)

    override def iterator: Iterator[(String, Any)] = self.iterator

    override def -(key: String): Map[String, Any] = this.copy(self = self - key)
  }

}
