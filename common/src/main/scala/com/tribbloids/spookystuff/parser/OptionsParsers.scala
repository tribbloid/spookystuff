package com.tribbloids.spookystuff.parser

import scala.util.parsing.combinator.RegexParsers

object OptionsParsers extends RegexParsers {

  //  override protected val whiteSpace = """[\s]+""".r

  override val skipWhitespace = false

  private val key: Parser[String] = "(?m)[^=]+".r
  private val value: Parser[String] = "[^\\s]*".r

  //  private val singleQuotedValue = "'" ~> value <~ "'"// TODO: these doesn't work, thus key='a b' can't be parse properly, why
  //  private val doubleQuotedValue = "\"" ~> value <~ "\""
  //  private val allValue = singleQuotedValue | doubleQuotedValue | value

  def stripQuotes(str: String, quote: String) = {

    if (str.startsWith(quote) && str.endsWith(quote)) str.stripPrefix(quote).stripSuffix(quote)
    else str
  }

  def trimAndStripQuotes(str: String): String = {
    if (str == "NULL") null
    else Seq("'", "\"").foldLeft(str){
      (str, v) =>
        stripQuotes(str, v)
    }
  }

  val pair: Parser[Option[(String, Option[String])]] =
    (key ~ ("=".r ~> value).?).? ^^ {
      case None => None
      case Some(k ~ v) => Some(k.trim -> v.map(trimAndStripQuotes))
    }

  val pairs: Parser[Map[String, Option[String]]] = phrase(repsep(pair, whiteSpace)) ^^ (
    v =>
      Map(v.flatten: _*)
    )

  def apply(input: String): Map[String, Option[String]] = parseAll(pairs, input) match {
    case Success(plan, _) => plan
    case x => sys.error(x.toString)
  }
}