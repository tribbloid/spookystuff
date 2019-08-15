package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.ParsersBenchmark.Epoch
import com.tribbloids.spookystuff.parsing.ParsersBenchmark.UseFastParse.blacklist
import com.tribbloids.spookystuff.utils.{InterleavedIterator, Interpolation}
import fastparse._
import org.apache.spark.BenchmarkHelper

import scala.util.Random

class ParsersBenchmark extends FunSpecx {

  import Random._

  describe("sanity test") {

    ignore("speed shoudn't be affected by JVM warming up") {

      val list: List[String] = ParsersBenchmark.iiInEpoch.map { _ =>
        ParsersBenchmark.rndStr(nextInt(30)) +
          "${" +
          ParsersBenchmark.rndStr(nextInt(30)) +
          "}" +
          ParsersBenchmark.rndStr(nextInt(30))
      }

      val epochs: List[Epoch] = List(
        Epoch(list, "regex1")(_.useRegex()),
        Epoch(list, "regex2")(_.useRegex())
      )

      ParsersBenchmark.compare(epochs)
    }
  }

  it("replace 1") {

    val stream: List[String] = ParsersBenchmark.iiInEpoch.map { _ =>
      var str = ParsersBenchmark.rndStr(nextInt(30))

      for (i <- ParsersBenchmark.streamSize) {

        str += "${" +
          ParsersBenchmark.rndStr(nextInt(30)) +
          "}"
      }

      str += ParsersBenchmark.rndStr(nextInt(30))
      str
    }

    val epochs: List[Epoch] = List(
      Epoch(stream, "speed reference", skipResultCheck = true)(_.speedRef()),
      Epoch(stream, "regex")(_.useRegex()),
      Epoch(stream, "fastParse")(_.useFastParse()),
      Epoch(stream, "FSM")(_.useFSM())
//      Epoch(stream, "do nothing", skipResultCheck = true)(_.doNothing())
    )

    ParsersBenchmark.compare(epochs)
  }
}

object ParsersBenchmark {

  import scala.concurrent.duration._

  val numVPerEpoch: Int = Math.pow(2, 16).toInt
  val iiInEpoch: List[Int] = (0 until ParsersBenchmark.numVPerEpoch).toList

  val streamSize: Int = 2 ^ 10

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

//    {
//      println(p.visualise().ASCIIArt())
//      p
//    }
  }

  val interpolation = Interpolation("$")

  class UTRunner(val str: String) extends AnyVal {

    def replace: String => String = identity[String]

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

    //measuring speed only, result is jibberish
    def speedRef(): String = {
      str.foreach(_ => {})
      str
    }

    def doNothing(): String = ""
  }

  case class Epoch(
      list: List[String],
      name: String = "[UNKNOWN]",
      skipResultCheck: Boolean = false
  )(
      fn: ParsersBenchmark.UTRunner => String
  ) {

    val stream: Stream[UTRunner] = {

      val runners: List[UTRunner] = list.map { str =>
        new ParsersBenchmark.UTRunner(str)
      }

      runners.toStream
    }

    val convertedStream: Stream[String] = {

      stream.map { runner =>
        fn(runner)
      }
    }

    def run(i: Int): Unit = {
//      System.gc()
      convertedStream.foreach(_ => {})
    }
  }

  def compare(epochs: List[Epoch]): Unit = {

    val zipped = new InterleavedIterator(
      epochs
        .filterNot(_.skipResultCheck)
        .map { v =>
          v.convertedStream.iterator
        }
    )

    val withOriginal = epochs.head.convertedStream.iterator.zip(zipped)

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

    val benchmarkHelper = BenchmarkHelper(
      this.getClass.getSimpleName.stripSuffix("$"),
      valuesPerIteration = numVPerEpoch,
//      minNumIters = 2,
      warmupTime = 5.seconds,
      minTime = 60.seconds
//      outputPerIteration = true
//      output = None,
    )

    epochs.foreach { epoch =>
      benchmarkHelper.self.addCase(epoch.name)(epoch.run)
    }
    benchmarkHelper.self.run()
  }
}
