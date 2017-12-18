//package org.apache.spark.ml.dsl.utils.messaging
//
//trait Nested[+V] {
//
//  def children: Seq[Nested[V]]
//}
//
//case class NestedMap[V](map: Map[String, Either[V, Nested[V]]]) extends Nested[V] {
//
//  override def children: Seq[Nested[V]] = map.values.collect {
//    case Right(v) => v
//  }
//    .toSeq
//}
//
//case class NestedSeq[V](seq: Iterable[Either[V, Nested[V]]]) extends Nested[V] {
//
//  override def children: Seq[Nested[V]] = seq.collect {
//    case Right(v) => v
//  }
//    .toSeq
//}
//
//case class Nest[V](
//                    either: Either[V, Nested[V]]
//                  ) {
//
//
//}