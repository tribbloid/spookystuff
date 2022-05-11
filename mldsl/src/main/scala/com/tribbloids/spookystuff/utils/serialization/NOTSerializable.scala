package com.tribbloids.spookystuff.utils.serialization

import com.tribbloids.spookystuff.utils.serialization.BeforeAndAfterShipping.Container

import java.io.NotSerializableException

/**
  * Any subclass in the closure cleaned by Spark ClosureCleaner will trigger a runtime error.
  */
trait NOTSerializable extends BeforeAndAfterShipping {

  {
    trigger
  }

  private lazy val error =
    new NotSerializableException(s"${this.getClass.getCanonicalName} is NOT serializable")

  override def beforeDeparture(): Unit = {
    throw error
  }

  override def afterArrival(): Unit = {
    throw error
  }

  private lazy val trigger: Container[this.type] = Container(this)
}
