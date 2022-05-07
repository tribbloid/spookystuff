package org.apache.spark.ml.dsl

import com.tribbloids.spookystuff.tree.TreeView

import scala.annotation.tailrec

/**
  * Can be compressed into radix tree
  */
object TrieNode {

  private def buildChildren[K, V](
      map: Iterable[(Seq[K], V)],
      prefix: Seq[K] = Seq(),
      depth: Int = 0
  ): Seq[TrieNode[K, Option[V]]] = {

    val grouped = map.groupBy(_._1.head).toSeq.sortBy(v => "" + v._1)
    val result = grouped.map { triplet =>
      val key = prefix ++ Seq(triplet._1)
      val value = map.toMap.get(Seq(triplet._1))
      val children = buildChildren[K, V](
        triplet._2
          .map(tuple => tuple._1.slice(1, Int.MaxValue) -> tuple._2)
          .filter(_._1.nonEmpty),
        key,
        depth + 1
      )
      TrieNode(key, value, children, depth + 1)
    }
    result
  }

  def build[K, V](map: Iterable[(Seq[K], V)]): TrieNode[K, Option[V]] = {
    TrieNode(
      key = Nil,
      value = map.toMap.get(Nil),
      children = buildChildren(map),
      0
    )
  }
}

case class TrieNode[K, V](
    key: Seq[K],
    value: V,
    children: Seq[TrieNode[K, V]],
    depth: Int
) extends TreeView[TrieNode[K, V]] {

  @tailrec
  final def lastSingleDescendant: TrieNode[K, V] =
    if (this.children.size == 1) this.children.head.lastSingleDescendant
    else this

  def compact: TrieNode[K, V] = {
    this.transform {
      case vv if vv.children.size == 1 =>
        vv.lastSingleDescendant
    }
  }

  def pruneUp: TrieNode[K, V] = {
    this.transform {
      case vv if vv.children.size == 1 =>
        vv.copy(children = vv.children.map { v =>
          v.copy[K, V](key = vv.key).pruneUp
        })
      case vv if vv.children.size > 1 =>
        vv.copy(children = vv.children.map { v =>
          v.copy[K, V](key = vv.key ++ v.key.lastOption).pruneUp
        })
    }
  }

  def rebuildDepth(i: Int = 0): TrieNode[K, V] = {
    this.copy(
      depth = i,
      children = this.children.map { child =>
        child.rebuildDepth(i + 1)
      }
    )
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[TrieNode[K, V]]): TrieNode[K, V] = {
    this.copy(children = newChildren)
  }
}
