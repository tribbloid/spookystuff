package com.tribbloids.spookystuff.commons.data

import ai.acyclic.prover.commons.multiverse.{CanUnapply, UnappliedForm}
import com.tribbloids.spookystuff.commons.DSLUtils

import java.lang.reflect.{InvocationTargetException, Method}
import scala.reflect.ClassTag

case class ReflCanUnapply[TT]()( // TODO: compile-time parsing is janky, should be replaced shortly
    implicit
    tcc: ClassTag[TT]
) extends CanUnapply[TT] {

  @transient lazy val clazz: Class[?] = tcc.runtimeClass

  @transient lazy val validGetters: Array[(String, Method)] = {

    val methods = clazz.getMethods

    val _methods = methods.filter { m =>
      (m.getParameterTypes.length == 0) &&
      DSLUtils.isSerializable(m.getReturnType)
    }
    val commonGetters: Array[(String, Method)] = _methods
      .filter { m =>
        m.getName.startsWith("get")
      }
      .map(v => v.getName.stripPrefix("get") -> v)
    val booleanGetters: Array[(String, Method)] = _methods
      .filter { m =>
        m.getName.startsWith("is")
      }
      .map(v => v.getName -> v)

    (commonGetters ++ booleanGetters).sortBy(_._1)
  }

  override def unapply(v: TT): Option[UnappliedForm] = {

    val kvs: Seq[(String, Any)] = validGetters.flatMap { tuple =>
      try {
        tuple._2.setAccessible(true)
        Some(tuple._1 -> tuple._2.invoke(v))
      } catch {
        case _: InvocationTargetException => None
      }
    }.toList

    val result = UnappliedForm.Pairs(
      kvs.map {
        case (k, v) =>
          Some(k) -> v
      }
    )

    Some(result)
  }
}
