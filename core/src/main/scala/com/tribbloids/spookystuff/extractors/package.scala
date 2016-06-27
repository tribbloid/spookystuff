package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.row.FetchedRow
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.language.implicitConversions

/**
  * Created by peng on 12/2/14.
  */
package object extractors {

  type TypeTag[T] = ScalaReflection.universe.TypeTag[T]

  type FR = FetchedRow

  type Extractor[+R] = GenExtractor[FR, R]

  type Resolved[+R] = GenResolved[FR, R]
  def Resolved = GenResolved


  //TODO: clean up, didn't fix the problem
//  implicit def Closure1[T, R](self: T => R): Function1[T, R] = self match {
//    case v: Function1[T, R] => v
//    case _ => new Function1[T, R] {
//      override def apply(v1: T): R = self.apply(v1)
//    }
//  }
//  trait Function1[-T, +R] extends scala.Function1[T, R] with Serializable {
//  }
//
//  implicit def PartialClosure[T, R](self: scala.PartialFunction[T, R]): PartialFunction[T, R] = self match {
//    case v: PartialFunction[T, R] => v
//    case _ => new PartialFunction[T, R] {
//
//      override def isDefinedAt(x: T): Boolean = self.isDefinedAt(x)
//      override def apply(v1: T): R = self.apply(v1)
//      override def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = self.applyOrElse(x, default)
//      override def lift = self.lift
//    }
//  }
//
//  trait PartialFunction[-T, +R] extends scala.PartialFunction[T, R] with Function1[T, R] {
//
////    abstract override def lift: Function1[T, R]
//  }

//  abstract class AbstractPartialFunction[-T, +R] extends scala.runtime.AbstractPartialFunction[T, R] with PartialFunction[T, R]
}