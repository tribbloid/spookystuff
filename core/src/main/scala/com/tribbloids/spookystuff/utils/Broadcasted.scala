package com.tribbloids.spookystuff.utils

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.language.implicitConversions

/**
  * Created by peng on 27/01/17.
  */
case class Broadcasted[T](
    @transient v: T
)(implicit
  @transient val sc: SparkContext = SparkContext.getOrCreate())
    extends ShippingMarks {

  @transient protected implicit val vCtg: ClassTag[T] = ClassTag(v.getClass)

  var delegate: Broadcast[T] = sc.broadcast(v)

  def rebroadcast(): Unit = {
    requireNotShipped()
    try {
      delegate.destroy()
    } catch {
      case e: Exception =>
        LoggerFactory.getLogger(this.getClass).error("broadcast cannot be destroyed", e)
    }
    delegate = sc.broadcast(v)
  }

  def value: T = Option(v).getOrElse {
    delegate.value
  }
}

object Broadcasted {

  implicit def asValue[T](v: Broadcast[T]): T = v.value
}
