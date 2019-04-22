package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.ParsersBenchmark.Epoch
import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.utils.{InterleavedIterator, Interpolation}
import org.apache.spark.BenchmarkHelper

import scala.util.Random

class ParsersBenchmark extends FunSpecx {

  import fastparse._

  import Random._

  describe("sanity test") {

    ignore("parser") {

      val result: String = parse("abcde$", ParsersBenchmark.to_$.result(_)).get.value
      result.shouldBe()
    }

    it("speed shoudn't be affected by JVM warming up") {

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
      ParsersBenchmark.rndStr(nextInt(30)) +
        "${" +
        ParsersBenchmark.rndStr(nextInt(30)) +
        "}" +
        ParsersBenchmark.rndStr(nextInt(30))
    }

    val epochs: List[Epoch] = List(
      Epoch(stream, "regex")(_.useRegex()),
      Epoch(stream, "fastParse")(_.useFastParse())
    )

    ParsersBenchmark.compare(epochs)
  }
}

object ParsersBenchmark {

  import fastparse._
  import NoWhitespace._

  val blacklist = "{}$/\\".toSet

  def rndStr(len: Int): String = {
    val charSeq = for (i <- 1 to len) yield {

      Random.nextPrintableChar()
    }

    charSeq
      .filterNot(v => blacklist.contains(v))
      .mkString("")
  }

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

  val interpolation = Interpolation("$")

  case class UnitRunner(str: String)(replace: String => String) {

    def useFastParse(verbose: Boolean = false): String = {

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

    def useRegex(): String = {

      interpolation(str)(replace)
    }
  }

  case class Epoch(strs: Stream[String], name: String = "")(
      fn: ParsersBenchmark.UnitRunner => String
  ) {

    val converted: Stream[String] = strs.map { str =>
      val runner = ParsersBenchmark.UnitRunner(str)(identity)
      fn(runner)
    }

    val closure: Int => Unit = { i: Int =>
      System.gc()
      print(name + " start profiling ...")
      val result = converted.foreach(_ => UnitRunner)
      print(" Done\n")
      result
    }
  }

  def compare(epochs: List[Epoch]): Unit = {

    val zipped = new InterleavedIterator(
      epochs.map { v =>
        v.converted.iterator
      }
    )

    val withOriginal = epochs.head.strs.iterator.zip(zipped)

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

    val benchmarkHelper = BenchmarkHelper(this.getClass.getSimpleName.stripSuffix("$"), 3)
    epochs.foreach { ee =>
      benchmarkHelper.self.addCase(ee.name)(ee.closure)
    }
    benchmarkHelper.self.run()
  }
}
