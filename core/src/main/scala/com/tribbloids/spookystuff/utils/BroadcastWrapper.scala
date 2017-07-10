package com.tribbloids.spookystuff.utils

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * Created by peng on 27/01/17.
  */
case class BroadcastWrapper[T: ClassTag](
                                          @transient sc: SparkContext,
                                          @transient v: T
                                        ) extends ShippingMarks {

  var broadcasted: Broadcast[T] = sc.broadcast(v)

  def rebroadcast(): Unit = {
    requireNotShipped()
    try {
      broadcasted.destroy()
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).error("broadcast cannot be destroyed", e)
    }
    broadcasted = sc.broadcast(v)
  }

  def value: T = Option(v).getOrElse {
    broadcasted.value
  }
}
