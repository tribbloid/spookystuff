package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import com.tribbloids.spookystuff.utils.{CommonUtils, TreeThrowable}

import scala.util.Try

/**
  * Created by peng on 14/01/17.
  */
trait ConflictDetection extends Cleanable {

  def _resourceIDs: Map[String, Set[_]]
  def _resourceQualifier: String = this.getClass.getCanonicalName

  final def resourceIDs: Map[String, Set[Any]] = _resourceIDs.map { tuple =>
    val rawK =
      if (tuple._1.isEmpty) null
      else tuple._1
    val k = CommonUtils./:/(_resourceQualifier, rawK)
    val v = tuple._2.map(_.asInstanceOf[Any])
    k -> v
  }
}

object ConflictDetection {

  def conflicts: Seq[Try[Unit]] = {

    val allObj = Cleanable.All.typed[ConflictDetection]

    val allResourceIDs: Map[String, Seq[Any]] = allObj
      .map {
        _.resourceIDs.mapValues(_.toSeq).toMap
      }
      .reduceOption { (m1, m2) =>
        val keys = (m1.keys ++ m2.keys).toSeq.distinct
        val kvs = keys.map { k =>
          val v = m1.getOrElse(k, Nil) ++ m2.getOrElse(k, Nil)
          k -> v
        }
        Map(kvs: _*)
      }
      .getOrElse(Map.empty)

    allResourceIDs.toSeq
      .flatMap { tuple =>
        tuple._2
          .groupBy(identity)
          .values
          .map { vs =>
            Try {
              assert(
                vs.size == 1, {
                  s"""
                         |${tuple._1}: resource ${vs.head} is used by ${vs.size} objects:
                         |${allObj
                       .filter(
                         v =>
                           v.resourceIDs
                             .getOrElse(tuple._1, Set.empty)
                             .contains(vs.head)
                       )
                       .map(v => s"$v -> ${vs.head}")
                       .mkString("\n")}
                     """.stripMargin
                }
              )
            }
          }
      }
  }

  def detectConflict(extra: Seq[Throwable] = Nil): Unit = {

    TreeThrowable.&&&(conflicts, extra = extra)
  }
}
