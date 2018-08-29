package com.tribbloids.spookystuff.testbeans

import java.io.NotSerializableException

import com.tribbloids.spookystuff.utils.{AssertSerializable, NOTSerializable}
import org.scalatest.Assertions

case class WithID(_id: Int) {
  override def toString: String = _id.toString
}

class NOTSerializableID(_id: Int) extends WithID(_id) with NOTSerializable {

  Assertions.intercept[NotSerializableException] {
    AssertSerializable(this)
  }
}
object NOTSerializableID {
  def apply(_id: Int) = new NOTSerializableID(_id)
}