package com.tribbloids.spookystuff.utils

import org.apache.spark.ml.dsl.utils.FlowUtils

trait ShippingMarks extends Serializable {

  /**
    * a shipped object is a deep copy of another object through serailization & deserialization.
    * potentially on another JVM or machine.
    * the @transient val is discarded during the process, so its value will
    */
  @transient private val shippingMark = Nil

  /**
    * due to limitation of some serialization libraries a shipped object may become a ZOMBIE REPLICA,
    * of which main constructor is bypassed when being deserialized.
    * this is particularly dangerous if the zombie and original object are on the same machine,
    * as the zombie becomes GC vulnerable and clean up the original object's resources
    */
  @transient private var zombieMark: Nil.type = _
  zombieMark = Nil

  def isShipped = shippingMark == null
  def notShipped = !isShipped

  def requireNotShipped() = {
    val methodName = FlowUtils.callerMethodName()

    require(notShipped, s"method $methodName can only be used on Spark driver, it is disabled after shipping")
  }

  def isZombie = zombieMark == null
  def notZombie = !isZombie
}
