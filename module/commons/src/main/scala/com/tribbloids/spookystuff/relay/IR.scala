package com.tribbloids.spookystuff.relay

import com.tribbloids.spookystuff.relay.io.Encoder

import scala.language.implicitConversions

trait IR extends RootTagged {

  type Body
  def body: Body

  def rootTagOvrd: Option[String]
}

object IR {

  type Aux[D] = IR { type Body = D }

//  implicit def fromValue[V](v: V): TreeIR.Leaf[V] = TreeIR.leaf(v)

  trait _Relay[I <: IR] extends Relay.Symmetric[I] {

    override type IR_>> = I

    override def toMessage_>>(v: I): I = v

    override def toProto_<<(v: I): I = v
  }

  implicit def toEncoder[T <: IR](ir: T): Encoder[T] = Encoder[T](ir)
}
