package com.tribbloids.spookystuff

import java.io.NotSerializableException

import com.tribbloids.spookystuff.testutils.AssertSerializable
import com.tribbloids.spookystuff.utils.NOTSerializable
import com.tribbloids.spookystuff.utils.locality.LocalityImplSuite.intercept

object TestBeans {

  case class WithID(_id: Int) {
    override def toString: String = _id.toString
  }

  def NOTSerializableID(_id: Int) = {

    val result = new WithID(_id) with NOTSerializable
    intercept[NotSerializableException] {
      AssertSerializable(result)
    }
    result
  }
}
