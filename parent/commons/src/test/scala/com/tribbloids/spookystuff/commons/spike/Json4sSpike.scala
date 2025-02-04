package com.tribbloids.spookystuff.commons.spike

import org.json4s.jackson.JsonMethods
import org.json4s.reflect.Reflector

import scala.collection.immutable.ListMap

//@Ignore
class Json4sSpike extends CodecSpike {
  import org.json4s.*

  // TODO: how to fix these two cases?

  implicit val fm: DefaultFormats.type = DefaultFormats

  implicit class example[T: Manifest](example: T) extends TestCodec[T] {

    {
      roundtripTest(example)
    }

    override def toJson(v: T): String = {

      val mani = Reflector.describe[T]

      JsonMethods.compact(Extraction.decompose(v))
    }

    override def fromJson(v: String): T = {
      Extraction.extract[T](JsonMethods.parse(v))
    }
  }

  describe("encode/decode") {

    val v1 = example(Map("a" -> 1, "b" -> 2))

    val v2 = example(ListMap("a" -> 1, "b" -> 2))

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
