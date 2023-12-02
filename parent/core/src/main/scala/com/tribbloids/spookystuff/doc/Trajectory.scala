package com.tribbloids.spookystuff.doc

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

case class Trajectory(
    source: Seq[Fetched],
    scopeRefs: Seq[Int],
    ordinal: Int = 0
) {

  lazy val inScope_Refs: Seq[(Fetched, Int)] = scopeRefs.map { i =>
    source(i) -> i
  }

  lazy val inScope: Seq[Fetched] = inScope_Refs.map(_._1)

  // make sure no pages with identical name can appear in the same group.
  lazy val splitByDistinctNames: Seq[Trajectory] = {
    val outerBuffer: ArrayBuffer[Seq[Int]] = ArrayBuffer()

    object Buffer {
      val refs: ArrayBuffer[Int] = ArrayBuffer[Int]()
      val names: mutable.HashSet[String] = mutable.HashSet[String]()

      def add(tuple: (Fetched, Int)): Unit = {
        refs += tuple._2
        names += tuple._1.name
      }

      def clear(): Unit = {
        refs.clear()
        names.clear()
      }

    }

    inScope_Refs.foreach { tt =>
      if (Buffer.names.contains(tt._1.name)) {
        outerBuffer += Buffer.refs.toList
        Buffer.clear()
      }
      Buffer.add(tt)
    }
    outerBuffer += Buffer.refs.toList // always left, have at least 1 member

    outerBuffer.zipWithIndex.map {
      case (v, i) =>
        this.copy(scopeRefs = v, ordinal = i)
    }.toSeq
  }
}

object Trajectory {

  def raw(source: Seq[Fetched]): Trajectory = Trajectory(source, source.indices)

  lazy val empty: Trajectory = raw(Nil)

  implicit def box(vs: Seq[Fetched]): Trajectory = raw(vs)

  implicit def unbox(v: Trajectory): Seq[Fetched] = v.inScope
}
