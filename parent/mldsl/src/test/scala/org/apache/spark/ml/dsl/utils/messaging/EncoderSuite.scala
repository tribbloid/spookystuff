package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.ml.dsl.utils.messaging.TestBeans._

class EncoderSuite extends FunSpecx {

  val user1 = User("1")
  val user2 = User("2", Some(Roles(Seq("r1", "r2"))))
  val map = Map(1 -> user1, 2 -> user2)

  describe("memberStr") {

    it("can print nested case classes") {
      val writer = Encoder(user1)
      writer.memberStrPretty.shouldBe(
        """
          |User(
          |  1,
          |  None
          |)
        """.stripMargin
      )
    }
    it("can print nested seq") {
      val writer = Encoder(user2)
      writer.memberStrPretty.shouldBe(
        """
          |User(
          |  2,
          |  Some(
          |    Roles(
          |      ::(
          |        r1,
          |        r2
          |      )
          |    )
          |  )
          |)
        """.stripMargin
      )
    }

    it("can print nested map") {
      val writer = Encoder(map)
      writer.memberStrPretty.shouldBe(
        """
          |Map2(
          |  1=User(
          |    1,
          |    None
          |  ),
          |  2=User(
          |    2,
          |    Some(
          |      Roles(
          |        ::(
          |          r1,
          |          r2
          |        )
          |      )
          |    )
          |  )
          |)
        """.stripMargin
      )
    }
    it("can resolve relay of nested object if augmented by AutomaticRelay") {
      val wrapper = Relayed2(Relayed("abc"))
      wrapper.memberStrPretty.shouldBe(
        """
          |Relayed2(
          |  abc
          |)
        """.stripMargin
      )
    }
  }
}
