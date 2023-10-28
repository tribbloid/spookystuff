package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

abstract class Fold[X, Y, R]() extends Extractor[R] {

  val _1: Extractor[X]

  val _2: Extractor[Y]

  def foldType(
      t1: => DataType,
      t2: => DataType
  ): DataType

  def fold(
      v1: => Option[X],
      v2: => Option[Y]
  ): Option[R]

  override def _args: Seq[GenExtractor[_, _]] = Seq(_1, _2)

  override def resolveType(tt: DataType): DataType = {
    lazy val t1 = _1.resolveType(tt)
    lazy val t2 = _2.resolveType(tt)

    foldType(t1, t2)
  }

  override def resolve(tt: DataType): PartialFunction[FR, R] = {
    lazy val oldResolved = _1.resolve(tt).lift
    lazy val newResolved = _2.resolve(tt).lift

    Unlift { v1: FR =>
      lazy val oldOpt = oldResolved.apply(v1)
      lazy val newOpt = newResolved.apply(v1)

      fold(oldOpt, newOpt)
    }
  }
}

object Fold {

  trait ResolveConflict[A, B] extends Fold[A, B, B] {}

  case class FailFast[R](_1: Extractor[Any], _2: Extractor[R]) extends ResolveConflict[Any, R] {

    override def foldType(t1: => DataType, t2: => DataType): DataType = {
      Try(t1) match {
        case Failure(_) => t2
        case Success(_) =>
          throw new UnsupportedOperationException(s"old field ${_1} already exists")
      }
    }

    override def fold(v1: => Option[Any], v2: => Option[R]): Option[R] = {
      v2
    }
  }

  trait Prefer[R] extends ResolveConflict[R, R] {

    override def foldType(t1: => DataType, t2: => DataType): DataType = {
      TypeCoercion
        .findTightestCommonType(t1, t2)
        .getOrElse(
          throw new IllegalArgumentException(
            s"""
               |Cannot fold, inconsistent type:
               |left: ${_1}
               |... with type $t1
               |right: ${_2}
               |... with type $t2
             """.stripMargin
          )
        )
    }
  }

  case class PreferNew[R](_1: Extractor[R], _2: Extractor[R]) extends Prefer[R] {

    override def fold(v1: => Option[R], v2: => Option[R]): Option[R] = {
      (v1.toSeq.map(v => v: R) ++ v2.toSeq).lastOption
    }
  }

  case class AppendSeq[T: ClassTag](
      get: Get,
      _2: Extractor[T]
  ) extends Fold[Seq[Any], T, Seq[T]]() {

    override val _1: Extractor[Seq[Any]] = get.AsSeq

    override def foldType(t1: => DataType, t2: => DataType): DataType = {
      ArrayType(t2)
    }

    override def fold(v1: => Option[Seq[Any]], v2: => Option[T]): Option[Seq[T]] = {

      val result = v1.toSeq.flatMap { old =>
        SpookyUtils.asIterable[T](old)
      } ++ v2

      Some(result)
    }

  }
}
