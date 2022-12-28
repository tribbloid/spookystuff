package com.tribbloids.spookystuff.relay

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

  object Multipart extends Relay.ToSelf[Multipart] {}

  // case object ObjectExample1 extends AbstractObjectExample

  case class Relayed(str: String)

  object Relayed extends Relay.ToMsg[Relayed] {

    override type Msg = String

    override def toMessage_>>(v: Relayed): IR.Aux[String] = TreeIR.leaf(v.str)

    override def toProto_<<(v: IR.Aux[String]): Relayed = ???
  }

  case class Relayed2(
      v: Relayed,
      vs1: Seq[Relayed] = Nil,
      vs2: Map[Int, Relayed] = Map.empty
  ) extends MessageAPI

  object Relayed2 extends AutomaticRelay[Relayed2] {}
}
