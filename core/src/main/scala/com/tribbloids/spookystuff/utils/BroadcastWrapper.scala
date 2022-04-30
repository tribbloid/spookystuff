package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by peng on 27/01/17.
  */
case class BroadcastWrapper[T](
    @transient var v: T
)(
    implicit
    @transient val sc: SparkContext = SparkContext.getOrCreate()
) extends Cleanable
    with ShippingMarks {

  protected def doBroadcast(): Broadcast[T] = {
    requireNotShipped()

    val v = this.v
    implicit def vCtg: ClassTag[T] = ClassTag(v.getClass)
    sc.broadcast(v)
  }

  @volatile var wrapped: Broadcast[T] = doBroadcast()

  def rebroadcast(): Unit = this.synchronized {
    requireNotShipped()
    destroy()
    wrapped = doBroadcast()
  }

  private def destroy(): Unit = this.synchronized {
    try {
      Option(wrapped).foreach(_.destroy())
    } catch {
      case _: NullPointerException | _: SparkException =>
      // Spark master is gone, no need to destroy
      case e: Exception =>
        LoggerFactory.getLogger(this.getClass).error("broadcast cannot be destroyed", e)
    }
  }

  def value: T = Option(v).getOrElse(
    wrapped.value
  )

  /**
    * can only be called once
    */
  override protected def cleanImpl(): Unit = {
    if (notShipped) destroy()
  }
}

object BroadcastWrapper {

  implicit def unbox[T](v: Broadcast[T]): T = v.value
}
