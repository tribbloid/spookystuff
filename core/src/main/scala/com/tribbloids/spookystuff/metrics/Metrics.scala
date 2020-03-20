package com.tribbloids.spookystuff.metrics

import org.apache.spark.ml.dsl.utils.NestedMap
import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils
import org.apache.spark.util.AccumulatorV2

import scala.language.implicitConversions

/**
  * Created by peng on 03/10/15.
  */
object Metrics {}

@SerialVersionUID(-32509237409L)
abstract class Metrics extends MetricLike {

//  def sparkContext: SparkContext = SparkContext.getOrCreate()

  @transient lazy val caseAccessorMapProto: List[(String, Any)] = ReflectionUtils.getCaseAccessorMap(this)

  /**
    * slow, should not be used too often
    */
  def getCaseAccessorMap(useDisplayName: Boolean = true): List[(String, Any)] = {

    if (!useDisplayName) caseAccessorMapProto
    else {
      caseAccessorMapProto.map {
        case (_, v: MetricLike) =>
          val name = v.displayName
          name -> v

        case others @ _ => others
      }
    }
  }

  //Only allowed on Master
  def resetAll(): Unit = {

    this.productIterator.toList.foreach {
      case acc: Acc[_] =>
        acc.reset()
      case _ =>
    }
  }

  case class View[T](
      fn: Acc[_ <: AccumulatorV2[_, _]] => Option[T],
      useDisplayName: Boolean = true
  ) {

    def toNestedMap: NestedMap[T] = {
      val result = NestedMap[T]()
      val caseAccessors = getCaseAccessorMap(useDisplayName)
      caseAccessors.foreach {
        case (_name: String, acc: Acc[_]) =>
          fn(acc).foreach(v => result += _name -> Left(v))

        case (_name: String, nested: Metrics) =>
          val name = nested.displayNameOvrd.getOrElse(_name)
          val nestedView = nested.View(fn, useDisplayName)
          result += name -> Right(nestedView.toNestedMap)

        case _ =>
          None
      }
      result
    }

    def toMap: Map[String, T] = toNestedMap.leafMap
  }

  object View extends View[Any](v => Some(v.value), true)

  def toNestedMap: NestedMap[Any] = View.toNestedMap
  def toMap: Map[String, Any] = View.toMap
}
