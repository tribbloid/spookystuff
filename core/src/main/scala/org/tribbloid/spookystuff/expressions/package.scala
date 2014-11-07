package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.utils._

/**
 * Created by peng on 10/14/14.
 */
package object expressions {

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

        override def apply(pageRow: PageRow): String = {

          val builder = new StringBuilder

          for (i <- 0 to fs.size) {
            builder.append(Utils.interpolateFromMap(parts(i),pageRow.cells))
            builder.append(fs(i)(pageRow))
          }

          builder.mkString
        }

        override def name: String = {

          val fStrs = fs.map(_.toString())

          parts.zip(fStrs).map(tuple => tuple._1+tuple._2).mkString
        }
      }
    }
  }
}
