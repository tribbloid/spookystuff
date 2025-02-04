package com.tribbloids.spookystuff.commons.spike

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule, JavaTypeable}

import scala.collection.immutable.ListMap

object JacksonScalaSpike {}

//@Ignore
class JacksonScalaSpike extends CodecSpike {

  val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  implicit class example[T: Manifest: JavaTypeable](example: T) extends TestCodec[T] {

    {
      roundtripTest(example)
    }

    override def toJson(v: T): String = {
      val tree = mapper.valueToTree[JsonNode](v)

      mapper.writeValueAsString(v)
    }

    override def fromJson(v: String): T = mapper.readValue[T](v)
  }

  describe("encode/decode") {

    example(Map("a" -> 1, "b" -> 2))

    example(ListMap("a" -> 1, "b" -> 2))

    {
      import java.nio.file.Path
      example(Path.of("/www/google/com"))
    }

    {
      import org.apache.hadoop.fs.Path
      example(new Path("/www/google/com"))
    }
  }
}
