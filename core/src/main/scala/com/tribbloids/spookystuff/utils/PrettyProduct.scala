package com.tribbloids.spookystuff.utils

import java.io.File

import com.tribbloids.spookystuff.extractors.Literal

import scala.runtime.ScalaRunTime

/**
  * Created by peng on 01/04/16.
  */

object PrettyProduct {

  def product2String(
                     x: Product,
                     start: String = "(",
                     sep: String = ",",
                     end: String = ")",
                     verbose: String = ""
                   ): String = {
    x match {
      case v: Literal[_, _] => v.toString
      case _ =>
        val nonVerbose = x.productIterator
          .map {
            case vv: PrettyProduct => vv.product2String(start, sep, end ,"") //has verbose over
            case vv: Product => product2String(vv, start, sep, end, "")
            case vv@ _ => "" + vv
          }
          .mkString(x.productPrefix + start, sep, end)
        val result = nonVerbose + Option(verbose).filter(_.nonEmpty).map("\n" + _).getOrElse("")
        result
    }
  }
}

trait PrettyProduct extends Product {

  abstract override def toString = this.product2String()

  def toStringVerbose = this.product2String(detail = detail)

  def toString_\\\ = this.product2String(File.separator, File.separator, File.separator)
  def toString_/:/ = this.product2String("/", "/", "/")

  def detail: String = ""

  def product2String(
                      start: String = "(",
                      sep: String = ",",
                      end: String = ")",
                      detail: String = ""
                    ) = {

    PrettyProduct.product2String(this, start, sep, end, detail)
  }
}
