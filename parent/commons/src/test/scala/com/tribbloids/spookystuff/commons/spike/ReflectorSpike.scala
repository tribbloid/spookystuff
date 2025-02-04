package com.tribbloids.spookystuff.commons.spike

import ai.acyclic.prover.commons.debug.print_@
import com.tribbloids.spookystuff.testutils.BaseSpec
import org.json4s.DefaultFormats

import scala.reflect.Manifest

class ReflectorSpike extends BaseSpec {
  // test how Reflector handles different classes

  import org.json4s.reflect.*
  {

    implicit val formats = DefaultFormats

    val seq: Seq[Manifest[?]] = {
      {
        import java.nio.file.Path

        Seq(
          implicitly[Manifest[java.nio.file.Path]],
          Manifest.classType(Path.of("abc").getClass)
        )
      } ++ {
        import org.apache.hadoop.fs.Path

        Seq(
          implicitly[Manifest[org.apache.hadoop.fs.Path]],
          Manifest.classType(new Path("abc").getClass)
        )
      }
    }

    seq.zipWithIndex.map {
      case (v, i) =>
        val tt = Reflector.scalaTypeOf(v)
        it("" + tt + " " + i) {
          val described = Reflector.describeWithFormats(tt)(formats)
          print_@(described)
        }

    }
  }

}
