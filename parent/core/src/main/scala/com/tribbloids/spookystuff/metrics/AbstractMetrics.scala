package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.relay.TreeIR
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.language.implicitConversions

/**
  * Created by peng on 03/10/15.
  */
@SerialVersionUID(-32509237409L)
abstract class AbstractMetrics extends MetricLike {

  protected def _symbol2children: List[(String, Any)] = ReflectionUtils.getCaseAccessorMap(this)

  @transient final lazy val symbol2children: List[(String, Any)] = _symbol2children

  override def samenessDelegatedTo: (String, List[(String, Any)]) = displayName -> symbol2children

  /**
    * slow, should not be used too often
    */
  final def namedChildren(useDisplayName: Boolean = true): List[(String, Any)] = {

    if (!useDisplayName) symbol2children
    else {
      symbol2children.map {
        case (_, v: MetricLike) =>
          val name = v.displayName
          name -> v

        case others @ _ => others
      }
    }
  }

  // Only allowed on Master
  def resetAll(): Unit = {

    symbol2children.foreach {
      case (_, acc: Acc[_]) =>
        acc.reset()
      case _ =>
    }
  }

  case class View[T](
      fn: Acc[_ <: AccumulatorV2[_, _]] => Option[T],
      useDisplayName: Boolean = true
  ) {

    def toTreeIR: TreeIR.MapTree[String, T] = {
      val cache = mutable.LinkedHashMap.empty[String, TreeIR[T]]
      val list = namedChildren(useDisplayName)
      list.foreach {

        case (_name: String, acc: Acc[_]) =>
          fn(acc).foreach(v => cache += _name -> TreeIR.leaf(v))

        case (_name: String, nested: AbstractMetrics) =>
          val name = nested.displayNameOvrd.getOrElse(_name)
          val nestedView = nested.View(fn, useDisplayName)
          cache += name -> nestedView.toTreeIR

        case _ =>
          None
      }
      TreeIR.Builder(Some(AbstractMetrics.this.productPrefix)).map(cache.toSeq: _*)
    }

    def toMap: Map[String, T] = toTreeIR.pathToValueMap.map {
      case (k, v) => CommonUtils./:/(k: _*) -> v
    }
  }

  object View extends View[Any](v => Some(v.value), true)
  object View_AccessorName extends View[Any](v => Some(v.value), false)
}

object AbstractMetrics {

  implicit def asView(v: AbstractMetrics): v.View.type = v.View
}
