package com.tribbloids.spookystuff.parser

import org.apache.spark.sql.catalyst.SqlLexical

import scala.language.{implicitConversions, postfixOps}
import scala.util.Try
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

abstract class CommonParsers extends StandardTokenParsers with PackratParsers {

  case class KeyWord(str: String) {

    lazy val parser: Parser[String] = str
  }
  implicit def kw2Parser(kw: KeyWord): Parser[String] = kw.parser

  implicit class SymbolView(symbol: Symbol) {

    val $: KeyWord = {
      KeyWord(symbol.name.toLowerCase())
    }
  }

  implicit class ParseResultView[T](parseResult: this.ParseResult[T]) {

    def getOrError: T = parseResult.getOrElse{
      val noSuccess = parseResult.asInstanceOf[NoSuccess]
      val msg = noSuccess.msg
      val next = noSuccess.next
      throw new IllegalArgumentException(s"$msg\n" +
        s"${next.source.subSequence(0, next.offset)}" +
        s"{${next.source.subSequence(next.offset, next.rest.offset)}}" +
        s"${next.source.subSequence(next.rest.offset, next.source.length())}")
    }

    def toTry = Try(parseResult.getOrError)
    def toOption = toTry.toOption
  }

  implicit class ParserView[T](parser: this.Parser[T]) {

    lazy val ph: PackratParser[T] = phrase(parser)

    def tryParse(input: String): Try[T] = {

      val scanner = new lexical.Scanner(input)

      val result = ph.apply(scanner)
      result.toTry
    }

    def _replaceAll(keyword: KeyWord, input: Input, max: Int = Int.MaxValue)(
      fn: T => String
    ): String = {
      val extractor = matchUntilKeyword(keyword) ~ (keyword ~> parser) ^^ {
        case before ~ body =>
          val replaced = fn(body)
          before + replaced
      }

      extractor.apply(input) match {
        case Success(replaced, left: Input) if max > 1 =>
          replaced + _replaceAll(keyword, left, max - 1)(fn)
        case _ =>
          restInput.apply(input).getOrError
      }
    }

    def replaceAll(keyWord: KeyWord, input: String, max: Int = Int.MaxValue)(
      fn: T => String
    ): String = {
      val scanner = new lexical.Scanner(input)

      val result = _replaceAll(keyWord, scanner, max)(fn)
      result
    }
  }

  def getSQLLexical: SqlLexical = {
    val reservedWords: Array[String] = this
      .getClass
      .getMethods
      .filter(
        v =>
          v.getParameterTypes.length == 0 &&
            v.getReturnType == classOf[KeyWord]
      )
      .map(
        v =>
          v.invoke(this).asInstanceOf[KeyWord].str
      )

    val lexical = new SqlLexical
    lexical.initialize(reservedWords.distinct)
    lexical
  }

  // Returns the rest of the input string that are not parsed yet
  lazy val restInput: PackratParser[String] = new PackratParser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,
        in.drop(in.source.length())
      )
  }


  // Returns the whole input string
  lazy val wholeInput: PackratParser[String] = new PackratParser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(in.source.toString, in.drop(in.source.length()))
  }

  //string interpolation always enabled
  lazy val restPairOptions: PackratParser[Map[String, Option[String]]] = restInput ^^ {
    str =>
      OptionsParsers.apply(str)
  }

  def matchUntilKeyword(
                          kw: KeyWord,
                          maxItr: Int = Int.MaxValue
                        ): Parser[String] = new Parser[String] {

    def apply(in: Input): ParseResult[String] = {
      var current = in
      for (_ <- 1 to maxItr) {
        if (current.atEnd)
          return Failure(s"cannot find '$kw' in input", in)
        else if (current.first == lexical.Keyword(kw.str))
          return Success(
            in.source.subSequence(in.offset, current.offset).toString,
            current
          )
        else
          current = current.drop(1)
      }
      Failure(s"cannot find '$kw' in input within $maxItr tokens", in)
    }
  }
}
