package com.tribbloids.spookystuff.relay

import com.tribbloids.spookystuff.tree.TreeView
import com.tribbloids.spookystuff.relay.io.Decoder
import com.tribbloids.spookystuff.utils.refl.ReflectionUtils

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

trait TreeIR[LEAF] extends IR with Product {

  import TreeIR._

  def children: Seq[TreeIR[LEAF]]

  def upcast[_LEAF >: LEAF]: TreeIR[_LEAF]

  class DepthFirstTransform[V1 >: LEAF, V2, RES <: TreeIR[V2]](
      val downFn: TreeIR[V1] => TreeIR[V1],
      val onLeafFn: TreeIR.Leaf[V1] => TreeIR[V2],
      val upFn: TreeIR[V2] => RES
  ) {

    def execute: RES = {

      val afterDown: TreeIR[V1] = downFn(TreeIR.this.upcast[V1])

      val afterOnLeaves: TreeIR[V2] = afterDown match {
        case sub: MapTree[_, V1] =>
          sub.copy(
            sub.repr.map {
              case (k, v) =>
                k -> new v.DepthFirstTransform(downFn, onLeafFn, upFn).execute
            },
            sub.rootTagOvrd
          )
        case sub: ListTree[V1] =>
          sub.copy(
            sub.children.map { v =>
              new v.DepthFirstTransform(downFn, onLeafFn, upFn).execute
            },
            sub.rootTagOvrd
          )
        case ll: Leaf[V1] =>
          onLeafFn(ll)
      }

      val afterUp = upFn(afterOnLeaves)

      afterUp
    }

    def down[V1N >: LEAF](fn: TreeIR[V1N] => TreeIR[V1N]): DepthFirstTransform[V1N, V1N, TreeIR[V1N]] = { // reset onLeaf & up
      new DepthFirstTransform[V1N, V1N, TreeIR[V1N]](fn, identity _, identity _)
    }

    def onLeaves[V2N](fn: TreeIR.Leaf[V1] => TreeIR[V2N]): DepthFirstTransform[V1, V2N, TreeIR[V2N]] = { // reset up
      new DepthFirstTransform(downFn, fn, identity _)
    }

    def up[V2N >: V2, RES <: TreeIR[V2N]](fn: TreeIR[V2N] => RES): DepthFirstTransform[V1, V2N, RES] = {
      new DepthFirstTransform(downFn, onLeafFn.andThen(_.upcast[V2N]), fn)
    }
  }

  object DepthFirstTransform extends DepthFirstTransform[LEAF, LEAF, TreeIR[LEAF]](identity _, identity _, identity _)

  def pathToValueMap: Map[Seq[String], LEAF]

  def treeView: _TreeView = _TreeView(this)

  object explode {

    import ExplodeRules._

    def explodeStringMap(): TreeIR[Any] =
      DepthFirstTransform.down(stringMap orElse preserve).execute

    def explodeProductOrStringMap(): TreeIR[Any] =
      DepthFirstTransform.down(product orElse stringMap orElse preserve).execute
  }
}

object TreeIR {

  case class _TreeView(self: TreeIR[_]) extends TreeView.Immutable[_TreeView] {
    override lazy val nodeName: String = self.getClass.getSimpleName

    override lazy val children: Seq[_TreeView] = self.children.map(_TreeView)

    override def stringArgs: Iterator[Any] =
      if (children.isEmpty) self.productIterator
      else Iterator.empty
  }

  case class Leaf[LEAF](
      body: LEAF,
      override val rootTagOvrd: Option[String]
  ) extends TreeIR[LEAF] {

    type Body = LEAF

    override def rootTag: String = rootTagOvrd.getOrElse(RootTagged.Infer(body).default)

    override def children: Seq[TreeIR[LEAF]] = Nil

    override lazy val pathToValueMap: ListMap[Seq[String], LEAF] = ListMap(Nil -> body)

    override def upcast[_LEAF >: LEAF]: Leaf[_LEAF] = copy[_LEAF](body)
  }

  trait Trunk[LEAF] extends TreeIR[LEAF] {

    def repr: ListMap[_, TreeIR[LEAF]]

    override lazy val pathToValueMap: ListMap[Seq[String], LEAF] = {
      repr.flatMap {
        case (k, v) =>
          v.pathToValueMap.map {
            case (kk, v) =>
              (Seq("" + k) ++ kk) -> v
          }
      }
    }
  }

  case class ListTree[LEAF](
      override val children: Seq[TreeIR[LEAF]],
      override val rootTagOvrd: Option[String]
  ) extends Trunk[LEAF] {

    override def rootTag: String = rootTagOvrd.getOrElse("List")

    type Body = List[Any]
    override def body: List[Any] = children.map(_.body).toList

    override def repr: ListMap[Int, TreeIR[LEAF]] = ListMap(children.zipWithIndex.map(_.swap): _*)

    override def upcast[_LEAF >: LEAF]: ListTree[_LEAF] = copy(
      children = children.map(_.upcast[_LEAF])
    )
  }

  case class MapTree[KEY, LEAF](
      override val repr: ListMap[KEY, TreeIR[LEAF]],
      override val rootTagOvrd: Option[String],
      isSchemaless: Boolean = true
  ) extends Trunk[LEAF] {

    override def rootTag: String = rootTagOvrd.getOrElse("Map")

    type Body = ListMap[KEY, Any]

    override lazy val body: Body = {

      val view = repr.map {
        case (k, v) =>
          k -> v.body
      }
      view
    }

    override lazy val children: Seq[TreeIR[LEAF]] = repr.values.toSeq

    lazy val schematic: MapTree[KEY, LEAF] = this.copy(isSchemaless = false)
    lazy val schemaless: MapTree[KEY, LEAF] = this.copy(isSchemaless = true)

    override def upcast[_LEAF >: LEAF]: MapTree[KEY, _LEAF] = copy[KEY, _LEAF](
      repr.map {
        case (k, v) => k -> v.upcast[_LEAF]
      }
    )
  }

  case class Builder(rootTagOvrd: Option[String] = None) {

    def leaf[V](v: V): Leaf[V] = {

      Leaf(v, rootTagOvrd)
    }

    def list[V](vs: TreeIR[_ <: V]*): ListTree[V] = {
      val _kvs = vs.map { v =>
        v.upcast[V]
      }

      ListTree(_kvs.toList, rootTagOvrd)
    }

    def map[K, V](kvs: (K, TreeIR[_ <: V])*): MapTree[K, V] = {
      val _kvs = kvs.map {
        case (k, v) =>
          k -> v.upcast[V]
      }

      MapTree(ListMap(_kvs: _*), rootTagOvrd)
    }

  }

  object EmptyBuilder extends Builder()

  implicit def asDefaultBuilder(self: this.type): Builder = EmptyBuilder

  object ExplodeRules {

    protected def mapToFlatStruct(vs: Iterable[(String, Any)], tagOvrd: Option[String]): MapTree[String, Any] = {
      val seq = vs.toSeq
      val seqToLeaf: Seq[(String, Leaf[Any])] = seq.map {
        case (k, v) =>
          k -> EmptyBuilder.leaf(v)
      }
      Builder(tagOvrd).map(
        seqToLeaf: _*
      )
    }

    def preserve: PartialFunction[TreeIR[Any], TreeIR[Any]] = {

      case v => v
    }

    def stringMap: PartialFunction[TreeIR[Any], TreeIR[Any]] = {

      case Leaf(map: collection.Map[String, Any], tag) =>
        mapToFlatStruct(map, tag)
    }

    def product: PartialFunction[TreeIR[Any], TreeIR[Any]] = {

      case Leaf(p: Product, tag) =>
        val accessors = ReflectionUtils.getCaseAccessorMap(p)
        mapToFlatStruct(accessors, tag).schematic
    }
  }

  object _Relay extends IR._Relay[TreeIR[Any]] {

    object DefaultDecoder extends Decoder.AnyTree

    implicit def asDecoderView(self: this.type): DecoderView = DecoderView(DefaultDecoder)
  }

}
