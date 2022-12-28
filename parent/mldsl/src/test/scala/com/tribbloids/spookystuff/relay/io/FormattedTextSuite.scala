package com.tribbloids.spookystuff.relay.io

import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.relay.TestBeans._

class FormattedTextSuite extends FunSpecx {

  val user1: User = User("1")
  val user2: User = User("2", Some(Roles(Seq("r1", "r2"))))
  val map: Map[Int, User] = Map(1 -> user1, 2 -> user2)

  describe(classOf[FormattedText].getSimpleName) {

    it("can print nested case classes") {
      val writer = FormattedText.forValue(user1)
      writer.text.shouldBe(
        """
          |User(
          |  "1",
          |  None
          |)
        """.stripMargin
      )
    }
    it("can print nested seq") {
      val writer = FormattedText.forValue(user2)
      writer.text.shouldBe(
        """
          |User(
          |  "2",
          |  Some(
          |    Roles(
          |      List(
          |        "r1",
          |        "r2"
          |      )
          |    )
          |  )
          |)
        """.stripMargin
      )
    }

    it("can print nested map") {
      val writer = FormattedText.forValue(map)
      writer.text.shouldBe(
        """
          |Map(
          |  1 -> User(
          |    "1",
          |    None
          |  ),
          |  2 -> User(
          |    "2",
          |    Some(
          |      Roles(
          |        List(
          |          "r1",
          |          "r2"
          |        )
          |      )
          |    )
          |  )
          |)
        """.stripMargin
      )
    }

    describe("treeText") {

      it("by Relay") {
        val v = Relayed("abc")
        v.treeText.shouldBe(
          """
            |"abc"
          """.stripMargin
        )
      }

      describe("by AutomaticRelay") {

        it("on value") {

          val v = Relayed2(Relayed("abc"))
          v.treeText.shouldBe(
            """
              |Relayed2
              |  "abc"
              |  List
              |  Map
            """.stripMargin
          )
        }

        it("on seq") {

          val v = Relayed2(
            Relayed("abc"),
            List(
              Relayed("1"),
              Relayed("2")
            )
          )
          v.treeText.shouldBe(
            """
              |Relayed2
              |  "abc"
              |  List
              |    "1"
              |    "2"
              |  Map
            """.stripMargin
          )
        }

        it("on seq and map") {

          val v = Relayed2(
            Relayed("abc"),
            List(
              Relayed("1"),
              Relayed("2")
            ),
            Map(
              3 -> Relayed("3"),
              4 -> Relayed("4")
            )
          )
          v.treeText.shouldBe(
            """
              |Relayed2
              |  "abc"
              |  List
              |    "1"
              |    "2"
              |  Map
              |    3 -> "3"
              |    4 -> "4"
            """.stripMargin
          )
        }
      }
    }

  }
}

object FormattedTextSuite {}
