package com.tribbloids.spookystuff.commons.serialization

import com.tribbloids.spookystuff.commons.serialization.BeforeAndAfterShipping.Trigger

import java.io.NotSerializableException

/**
  * Any subclass in the closure cleaned by Spark ClosureCleaner will trigger a runtime error.
  */
trait NOTSerializable extends BeforeAndAfterShipping {

  import NOTSerializable._

  {
    trigger
  }

  private lazy val trigger = Trigger(Internal())
}

object NOTSerializable {

  case class Internal() extends BeforeAndAfterShipping {

    private lazy val error =
      new NotSerializableException(s"${this.getClass.getCanonicalName} is NOT serializable")

    override def beforeDeparture(): Unit = {
      throw error
    }

    override def afterArrival(): Unit = {
      throw error
    }
  }
}
