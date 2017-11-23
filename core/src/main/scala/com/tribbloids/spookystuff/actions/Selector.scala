package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.selenium.BySizzleCssSelector
import com.tribbloids.spookystuff.utils.ScalaUDT
import org.apache.spark.ml.dsl.utils.messaging.MessageRelay
import org.apache.spark.sql.types.SQLUserDefinedType
import org.openqa.selenium.By

import scala.language.implicitConversions

object Selector extends MessageRelay[Selector]{

  implicit def fromString(v: String) = Selector(new BySizzleCssSelector(v))

  override type M = String

  override def toM(v: Selector) = v.toString
}

@SQLUserDefinedType(udt = classOf[SelectorUDT])
case class Selector(by: By) extends Selector.API {

  override def toString: String = by.toString
}

class SelectorUDT extends ScalaUDT[Selector]