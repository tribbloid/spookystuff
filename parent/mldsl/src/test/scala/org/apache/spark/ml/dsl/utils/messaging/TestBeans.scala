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

  // case object ObjectExample1 extends AbstractObjectExample

  case class Relayed(str: String)

  object Relayed extends Relay[Relayed] {
    override def toMessage_>>(v: Relayed): String = v.str

    override type Msg = String

    override def toProto_<<(v: String, rootTag: String): Relayed = ???
  }

  case class Relayed2(vs: Relayed) extends MessageAPI

  object Relayed2 extends AutomaticRelay[Relayed2] {}
}
