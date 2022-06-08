package org.apache.spark.ml.dsl.utils.messaging

import java.util.Date

object TestBeans {

  case class TimeWrapper(time: Date)

  case class UsersWrapper(a: String, users: Users)

  case class Users(user: Seq[User])

  case class User(
      name: String,
      roles: Option[Roles] = None
  )

  case class Roles(role: Seq[String])

  case class Multipart(a: String, b: String)(c: Int = 10)

  object Multipart extends MessageReader[Multipart] {}

  //case object ObjectExample1 extends AbstractObjectExample

  case class WithCodec(str: String)

  object WithCodec extends MessageRelay[WithCodec] {
    override def toMessage_>>(v: WithCodec) = v.str

    override type M = String
  }

  case class CodecWrapper(vs: WithCodec) extends MessageAPI

  object CodecWrapper extends AutomaticRelay[CodecWrapper]
}
