package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.ParsersBenchmark.Epoch
import com.tribbloids.spookystuff.parsing.ParsersBenchmark.UseFastParse.blacklist
import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.utils.{InterleavedIterator, Interpolation}
import org.apache.spark.BenchmarkHelper
import fastparse._

import scala.util.Random

class ParsersBenchmark extends FunSpecx {

  import fastparse._

  import Random._

  describe("sanity test") {

    ignore("parser") {

      val result: String = parse("abcde$", ParsersBenchmark.UseFastParse.to_$.result(_)).get.value
      result.shouldBe()
    }

    ignore("speed shoudn't be affected by JVM warming up") {

      val stream: Stream[String] = (0 to Math.pow(2, 8).toInt).toStream.map { _ =>
        ParsersBenchmark.rndStr(nextInt(30)) +
          "${" +
          ParsersBenchmark.rndStr(nextInt(30)) +
          "}" +
          ParsersBenchmark.rndStr(nextInt(30))
      }

      val epochs: List[Epoch] = List(
        Epoch(stream, "regex1")(_.useRegex()),
        Epoch(stream, "regex2")(_.useRegex())
      )

      ParsersBenchmark.compare(epochs)
    }

  }

  it("replace 1") {

    val stream: Stream[String] = (0 to Math.pow(2, 16).toInt).toStream.map { _ =>
      var str = ParsersBenchmark.rndStr(nextInt(30))

      for (i <- 0 to Random.nextInt(10)) {

        str += "${" +
          ParsersBenchmark.rndStr(nextInt(30)) +
          "}"
      }

      str += ParsersBenchmark.rndStr(nextInt(30))
      str
    }

    val epochs: List[Epoch] = List(
      Epoch(stream, "regex")(_.useRegex()),
      Epoch(stream, "fastParse")(_.useFastParse()),
      Epoch(stream, "FSM")(_.useFSM())
    )

    ParsersBenchmark.compare(epochs)
  }
}

object ParsersBenchmark {

  import scala.concurrent.duration._

  def rndStr(len: Int): String = {
    val charSeq = for (i <- 1 to len) yield {

      Random.nextPrintableChar()
    }

    charSeq
      .filterNot(v => blacklist.contains(v))
      .mkString("")
  }

  object UseFastParse {
    import NoWhitespace._

    val blacklist = "{}$/\\".toSet

    //  val allKWs: String = "/\\\\$}"

    case class UntilCharInclusive(kw: Char) {

      //    final val allKWs: String = "/\\\\" + kw

      def predicate(c: Char) = c != kw && c != '\\'
      def strChars[_: P] = P(CharsWhile(predicate))

      def escaped[_: P] = P("\\" ~/ AnyChar)
      def str[_: P] = (strChars | escaped).rep.!

      def result[_: P] = P(str ~ kw.toString) //TODO: why this cannot be ~/ ?
    }

    val to_$ = UntilCharInclusive('$')
    val `to_}` = UntilCharInclusive('}')

    def once[_: P] = P(to_$.result ~ "{" ~ `to_}`.result)

    //  def capturedOrEOS[_: P] = captured | End.!

    def nTimes[_: P] = P(once.rep ~/ AnyChar.rep.!) // last String should be ignored
  }

  object UseFSM {

    import FSMParserDSL._

    def escape(v: Parser[_]): Operand[FSMParserGraph.Layout.GG] = {

      val escapeChar = P_*('\\')
      escapeChar :& escapeChar :~> v
    }

    val first = escape(P_*('$').!-)

    val enclosed = escape(P_*('}').!-.^^ { v =>
      Some(v)
    })

    val p = first :~> P('{').-- :~> enclosed :& first :~> EOS_* :~> FINISH

    {
      println(p.visualise().ASCIIArt())
      p
    }
  }

  val interpolation = Interpolation("$")

  case class UTRunner(str: String)(replace: String => String) {

    def useFastParse(verbose: Boolean = false): String = {

      import UseFastParse._

      val parsed: (Seq[(String, String)], String) = parse(str, nTimes(_), verboseFailures = verbose) match {
        case v: Parsed.Failure =>
          throw new UnsupportedOperationException(
            s"""
               |Cannot parse:
               |$str
               |${if (verbose) v.longMsg else v.msg}
            """.stripMargin
          )
        case v @ _ =>
          v.get.value
      }

      val interpolated = parsed._1
        .flatMap {
          case (s1, s2) =>
            Seq(s1, replace(s2))
        }

      val result = (interpolated :+ parsed._2)
        .mkString("")

      result

    }

    def useFSM(): String = {
      import UseFSM._

      val parsed = p.parse(str)
      val interpolated: Seq[String] = parsed.exports.map {
        case v: String        => v
        case Some(vv: String) => replace(vv)
      }
      interpolated.mkString("")
    }

    def useRegex(): String = {

      interpolation(str)(replace)
    }
  }

  case class Epoch(stream: Stream[String], name: String = "")(
      fn: ParsersBenchmark.UTRunner => String
  ) {

    val convertedStream: Stream[String] = stream.map { str =>
      val runner = ParsersBenchmark.UTRunner(str)(identity)
      fn(runner)
    }

    val closure: Int => Unit = { i: Int =>
//      System.gc()
//      print(name + " start profiling ...")
      val result = convertedStream.foreach(_ => {})
//      print(" Done\n")
//      result
    }
  }

  def compare(epochs: List[Epoch]): Unit = {

    val zipped = new InterleavedIterator(
      epochs.map { v =>
        v.convertedStream.iterator
      }
    )

    val withOriginal = epochs.head.stream.iterator.zip(zipped)

    withOriginal.foreach {
      case (original, seq) =>
        Predef.assert(
          seq.distinct.size == 1,
          s"""
             |result mismatch!
             |original:
             |$original
             |${epochs.map(_.name).zip(seq).map { case (k, v) => s"$k:\n$v" }.mkString("\n")}
         """.stripMargin
        )
    }

    val benchmarkHelper = BenchmarkHelper(this.getClass.getSimpleName.stripSuffix("$"), minTime = 60.seconds)
    epochs.foreach { epoch =>
      benchmarkHelper.self.addCase(epoch.name)(epoch.closure)
    }
    benchmarkHelper.self.run()
  }
}
