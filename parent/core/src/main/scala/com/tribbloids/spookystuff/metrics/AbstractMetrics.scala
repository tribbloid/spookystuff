package com.tribbloids.spookystuff.metrics

import org.apache.spark.ml.dsl.utils.NestedMap
import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils
import org.apache.spark.util.AccumulatorV2

import java.lang.reflect.Modifier
import scala.language.implicitConversions
import scala.util.Try

/**
  * Created by peng on 03/10/15.
  */
@SerialVersionUID(-32509237409L)
abstract class AbstractMetrics extends MetricLike {

  protected def _symbol2children: List[(String, Any)] = ReflectionUtils.getCaseAccessorMap(this)

  @transient final lazy val symbol2children = _symbol2children

  override def _equalBy: (String, List[(String, Any)]) = displayName -> symbol2children

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

    def toNestedMap: NestedMap[T] = {
      val result = NestedMap[T]()
      val list = namedChildren(useDisplayName)
      list.foreach {
        case (_name: String, acc: Acc[_]) =>
          fn(acc).foreach(v => result += _name -> Left(v))

        case (_name: String, nested: AbstractMetrics) =>
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
  object View_AccessorName extends View[Any](v => Some(v.value), false)
}

object AbstractMetrics {

//  case object Empty extends AbstractMetrics

  // TODO: useless at the moment
  abstract class HasExtraMembers extends AbstractMetrics {

    def initialise(): Unit = {

      // lazy members has to be initialised before shipping
      extraMembers
    }

    final protected def writeReplace(): Any = {
      initialise()
      this
    }

    @transient private lazy val extraMembers: List[(String, MetricLike)] = {
      val methods = this.getClass.getMethods.toList
        .filter { method =>
          val parameterMatch = method.getParameterCount == 0
          val returnTypeMatch = classOf[MetricLike].isAssignableFrom(method.getReturnType)

          returnTypeMatch && parameterMatch
        }

      val publicMethods = methods.filter { method =>
        val mod = method.getModifiers
        !method.getName.startsWith("copy") && Modifier.isPublic(mod) && !Modifier.isStatic(mod)
      }

      val extra = publicMethods.flatMap { method =>
        Try {
          val value = method.invoke(this).asInstanceOf[MetricLike]
          if (value == null)
            throw new UnsupportedOperationException(s"member `${method.getName}` has not been initialised")

          if (value.eq(this) || value == null) {
            None
          } else {
            Some(method.getName -> value)
          }
        }.toOption.toSeq.flatten
      }

      extra
    }

    override protected def _symbol2children: List[(String, Any)] = {

      super._symbol2children ++ extraMembers
    }
  }

  implicit def asView(v: AbstractMetrics): v.View.type = v.View
}
