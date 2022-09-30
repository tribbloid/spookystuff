package com.tribbloids.spookystuff.utils.serialization

import com.tribbloids.spookystuff.utils.serialization.BeforeAndAfterShipping.Container

import java.io.NotSerializableException

trait NOTSerializable extends BeforeAndAfterShipping {

  import NOTSerializable._

  {
    trigger
  }

  private lazy val trigger = Container(Internal())
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
