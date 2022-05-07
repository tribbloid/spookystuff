package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.ml.dsl.utils.messaging.TestBeans._

class MessageWriterSuite extends FunSpecx {

  val user1 = User("1")
  val user2 = User("2", Some(Roles(Seq("r1", "r2"))))
  val map = Map(1 -> user1, 2 -> user2)

  describe("memberStr") {

    it("can print nested case classes") {
      val writer = MessageWriter(user1)
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
      val writer = MessageWriter(user2)
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
      val writer = MessageWriter(map)
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
    it("can resolve codec of nested object if augmented by AutomaticRelay") {
      val wrapper = CodecWrapper(WithCodec("abc"))
      wrapper.memberStrPretty.shouldBe(
        """
          |CodecWrapper(
          |  abc
          |)
        """.stripMargin
      )
    }
  }
}
