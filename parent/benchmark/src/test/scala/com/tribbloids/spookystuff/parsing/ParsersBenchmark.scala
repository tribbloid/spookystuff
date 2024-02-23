package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.commons.InterleavedIterator
import fastparse.internal.Logger
import org.apache.spark.benchmark.BenchmarkHelper
import org.scalatest.Ignore
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

@Ignore //TODO: enable!
class ParsersBenchmark extends BaseSpec {

  import com.tribbloids.spookystuff.parsing.ParsersBenchmark._

  def maxSectionLen: Int = 100
  def maxRepeat: Int = 1000

  lazy val seed: Long = Random.nextLong()

  def getRandomStrs: Stream[String] = {

    val gen = RandomStrGen(seed)
    gen.toStream
  }

  it("replace N") {

    val epochs: List[Epoch] = List(
      Epoch(getRandomStrs, "speed reference", skipResultCheck = true)(_.speedRef()),
      Epoch(getRandomStrs, "fastParse")(_.useFastParse()),
      Epoch(getRandomStrs, "FSM")(_.useFSM())
//      Epoch(stream, "do nothing", skipResultCheck = true)(_.doNothing())
    )

    ParsersBenchmark.compare(epochs)
  }
}

object ParsersBenchmark {

  import scala.concurrent.duration._

  val numVPerEpoch: Int = Math.pow(2, 16).toInt

  val streamRange: Range = 1 to 2 ^ 10

  object UseFastParse {

    val blacklist: Set[Char] = "{}$/\\".toSet

    case class Impl(log: ArrayBuffer[String] = ArrayBuffer.empty) {

      import fastparse._
      import NoWhitespace._

      implicit val logger: Logger = Logger(v => log += v)

      //  val allKWs: String = "/\\\\$}"

      case class UntilCharInclusive(kw: Char) {

        //    final val allKWs: String = "/\\\\" + kw

        def predicate(c: Char): Boolean = c != kw && c != '\\'
        def strChars[_: P]: P[Unit] = P(CharsWhile(predicate))

        def escaped[_: P]: P[Unit] = P("\\" ~/ AnyChar)
        def str[_: P]: P[String] = (strChars | escaped).rep.!

        def result[_: P]: P[String] = P(str ~ kw.toString).log
      }

      val to_$ : UntilCharInclusive = UntilCharInclusive('$')
      val `to_}`: UntilCharInclusive = UntilCharInclusive('}')

      def once[_: P]: P[(String, String)] = P(to_$.result ~ "{" ~ `to_}`.result)

      def nTimes[_: P]: P[(Seq[(String, String)], String)] =
        P(once.rep ~/ AnyChar.rep.!) // last String should be ignored

      def parser[_: P]: P[(Seq[(String, String)], String)] = nTimes.log

      def parseStr(str: String, verbose: Boolean = false): (Seq[(String, String)], String) = {

        parse(str, parser(_), verboseFailures = verbose) match {
          case v: Parsed.Failure =>
            throw new UnsupportedOperationException(
              s"""
                 |Cannot parse:
                 |$str
                 |${if (verbose) v.longMsg else v.msg}
                 | === error breakdown ===
                 |${log.mkString("\n")}
            """.stripMargin
            )
          case v @ _ =>
            v.get.value
        }

      }
    }
  }

  object UseFSM {

    import FSMParserDSL._

    def esc(v: Parser[_]): Operand[FSMParserGraph.Layout.GG] = {

      val esc = ESC('\\')
      esc :~> v
    }

    val first: Operand[FSMParserGraph.Layout.GG] = esc(P_*('$').!-)

    val enclosed: Operand[FSMParserGraph.Layout.GG] = esc(P_*('}').!-.^^ { io =>
      Some(io.outcome.`export`)
    })

    val fsmParser: Operand[FSMParserGraph.Layout.GG] = first :~> P('{').-- :~> enclosed :& first :~> EOS_* :~> FINISH

    {
      assert(
        fsmParser.visualise().ASCIIArt() ==
          """
            |          ╔═══════════════╗          ╔═══════════════╗            
            |          ║(TAIL>>-) [ ∅ ]║          ║(TAIL-<<) [ ∅ ]║            
            |          ╚═══════╤═══════╝          ╚═══════╤═══════╝            
            |                  │                          │                    
            |                  └───────────┐   ┌──────────┘                    
            |                              │   │                               
            |                              v   v                               
            |                          ╔═══════════╗                           
            |                          ║   ROOT    ║                           
            |                          ╚═╤═════╤═╤═╝                           
            |                            │ ^^  │ │                             
            |               ┌────────────┘ ││┌─┘ └─────────┐                   
            |               │            ┌─┼┘│             │                   
            |               v            │ │ │             │                   
            |       ╔══════════════╗     │ │ │             │                   
            |       ║[ '$' [0...] ]║     │ │ │             │                   
            |       ╚══╤═══════════╝     │ │ │             │                   
            |          │                 │ │ │             │                   
            |          v                 │ │ │             │                   
            |        ╔═══╗               │ │ │             │                   
            |        ║---║               │ │ │             │                   
            |        ╚═╤═╝               │ │ │             │                   
            |          │                 │ │ │             │                   
            |          v                 │ │ │             v                   
            |    ╔═══════════╗           │ │ │   ╔══════════════════╗          
            |    ║[ '{' [0] ]║           │ │ │   ║[ '[EOS]' [0...] ]║          
            |    ╚══════╤════╝           │ │ │   ╚══════════════╤═══╝          
            |           │                │ │ │                  │              
            |           │                │ │ └────────────┐     │              
            |           │                │ └─────────┐    │     │              
            |           v                │           │    │     v              
            |       ╔═══════╗            │           │    │ ╔══════╗           
            |       ║  ---  ║            │           │    │ ║FINISH║           
            |       ╚═╤═══╤═╝            │           │    │ ╚═══╤══╝           
            |         │ ^ │              │           │    │     │              
            |         │ │ └──────────┐   │           │    │     └───────┐      
            |         │ │            │   │           │    │             │      
            |         v │            v   │           │    v             v      
            | ╔═════════╧════╗ ╔═════════╧════╗ ╔════╧═════════╗ ╔════════════╗
            | ║[ '\' [0...] ]║ ║[ '}' [0...] ]║ ║[ '\' [0...] ]║ ║(HEAD) [ ∅ ]║
            | ╚══════════════╝ ╚══════════════╝ ╚══════════════╝ ╚════════════╝
            |""".stripMargin
            .split("\n")
            .toList
            .filterNot(_.replaceAllLiterally(" ", "").isEmpty)
            .mkString("\n")
      )
    }
  }

  case class RandomStrGen(
      seed: Long,
      override val size: Int = numVPerEpoch,
      maxSectionLen: Int = 100,
      maxRepeat: Int = 100
  ) extends Iterable[String] {

    val random: Random = new Random()

    import random._

    def rndStr(len: Int): String = {
      val charSeq = for (i <- 1 to len) yield {

        nextPrintableChar()
      }

      charSeq
        .filterNot(v => UseFastParse.blacklist.contains(v))
        .mkString("")
    }

    def generate: String = {

//      print("+")
      def getSectionStr = rndStr(nextInt(maxSectionLen))

      (0 to nextInt(maxRepeat)).map { _ =>
        getSectionStr +
          "${" +
          getSectionStr +
          "}" +
          getSectionStr
      }.mkString
    }

    lazy val base: Seq[Int] = 1 to size

    override def iterator: Iterator[String] = {

      random.setSeed(seed)
      base.iterator.map { _ =>
        generate
      }
    }
  }

  class UTRunner(val str: String) extends AnyVal {

    def replace: String => String = { str =>
      (0 until str.length).map(_ => "X").mkString("[", "", "]")
    }

    def useFastParse(verbose: Boolean = false): String = {

      val impl = UseFastParse.Impl()

      val parsed = impl.parseStr(str, verbose)

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

      val parsed: ParsingRun.ResultSeq = fsmParser.parse(str)

      val interpolated: Seq[String] = parsed.outputs.map {
        case v: String        => v
        case Some(vv: String) => replace(vv)
        case v @ _ =>
          sys.error(v.toString)
      }
      interpolated.mkString("")
    }

    // measuring speed only, result is jibberish
    def speedRef(): String = {
      str.map(identity)
//      str
    }

    def doNothing(): String = ""
  }

  case class Epoch(
      strs: Stream[String],
      name: String = "[UNKNOWN]",
      skipResultCheck: Boolean = false
  )(
      fn: ParsersBenchmark.UTRunner => String
  ) {

    val runners: Stream[UTRunner] = {

      val runners = strs.zipWithIndex.map {
        case (str, _) =>
          new ParsersBenchmark.UTRunner(str)
      }

      runners
    }

    val converted: Stream[String] = {

      runners.map { runner =>
        fn(runner)
      }
    }

    def run(i: Int): Unit = {
//      System.gc()
      converted.foreach(_ => {})
    }
  }

  def compare(epochs: List[Epoch]): Unit = {

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

    LoggerFactory.getLogger(this.getClass).info("=== Benchmark finished ===")

    val _epochs = epochs.filterNot(_.skipResultCheck)

    val zipped = new InterleavedIterator(
      _epochs
        .map { v =>
          v.converted.iterator
        }
    )

    val withOriginal = _epochs.head.converted.iterator.zip(zipped)

    withOriginal.foreach {
      case (original, seq) =>
        Predef.assert(
          seq.distinct.size == 1,
          s"""
             |result mismatch!
             |original:
             |$original
             |${_epochs.map(_.name).zip(seq).map { case (k, v) => s"$k:\n$v" }.mkString("\n")}
         """.stripMargin
        )
    }

  }
}
