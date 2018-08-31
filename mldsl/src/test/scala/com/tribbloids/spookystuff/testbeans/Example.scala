package com.tribbloids.spookystuff.testbeans

import org.apache.spark.ml.dsl.utils.refl.ScalaUDT
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  * Created by peng on 19/11/16.
  */
class GenericExample[T](
    val a: String,
    val b: T
) extends Serializable {

  lazy val c = a + b

  def fn: T = b
  def fn(i: T) = "" + b + i
  def fnBlock(x: T)(y: T, z: T) = "" + b + x + y + z

  def fnOpt(x: T): Option[T] = {
    if (x == null) None
    else if (b.hashCode() >= x.hashCode()) Some(x)
    else None
  }
  def fnOptOpt(x: Option[T], default: T): Option[T] = {
    fnOpt(x.getOrElse(default))
  }

  def fnDefault(
      a: T,
      b: String = "b"
  ) = "" + a + b

  def *=>(k: T): String = "" + k
}

class ExampleUDT extends ScalaUDT[Example]
@SQLUserDefinedType(udt = classOf[ExampleUDT])
class Example(
    override val a: String = "dummy",
    override val b: Int = 1
) extends GenericExample[Int](a, b)
