package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.utils._

import scala.language.implicitConversions

/**
 * Created by peng on 10/14/14.
 */
package object expressions {

//  def href(selector: String,
//           absolute: Boolean = true,
//           noEmpty: Boolean = true
//            ) = '*.href(selector,absolute,noEmpty)
//
//  def src(selector: String,
//          absolute: Boolean = true,
//          noEmpty: Boolean = true
//           ) = '*.src(selector,absolute,noEmpty)

  implicit def symbolToByKeyExpr(symbol: Symbol): ByKeyExpr = ByKeyExpr(symbol.name)

  implicit def stringToExpr(str: String): Expr[String] = {

    val delimiter = Const.keyDelimiter
    val regex = (delimiter+"\\{[^\\{\\}\r\n]*\\}").r

    if (regex.findFirstIn(str).isEmpty) {
      new Value[String](str)
    }
    else {
      new Expr[String] {

        name = str

        override def apply(v1: PageRow): String = Utils.interpolate(str, v1).orNull
      }
    }
  }

  implicit class StrExprHelper(val strC: StringContext) {

    private def parts = strC.parts

    def checkLengths(args: Seq[_]): Unit =
      if (parts.length != args.length + 1)
        throw new IllegalArgumentException("wrong number of arguments for interpolated string")

    //    def cell(fs: String*): Expr[Seq[String]] = {
    //      check
    //    }

    //    def last[T](fs: Extract[_]*): Expr[Seq[String]] = x(fs.map(LastPage(_)): _*)

    def x(fs: Expr[_]*): Expr[String] = {
      checkLengths(fs)

      new Expr[String] {

        name = {
          val fStrs = fs.map(_.name)

          parts.zip(fStrs).map(tuple => tuple._1+tuple._2).mkString
        }

        override def apply(pageRow: PageRow): String = {

          val builder = new StringBuilder

          for (i <- 0 to fs.size) {
            val value = fs(i)(pageRow)
            if (value == null ) return null
            builder.append(stringToExpr(parts(i))(pageRow))
            builder.append(value)
          }

          builder.mkString
        }
      }
    }
  }
}