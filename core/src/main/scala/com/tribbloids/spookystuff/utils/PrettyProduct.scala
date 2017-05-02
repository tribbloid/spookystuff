package com.tribbloids.spookystuff.utils

import java.io.File

import com.tribbloids.spookystuff.extractors.Literal
import org.apache.spark.ml.dsl.utils.Verbose

/**
  * Created by peng on 01/04/16.
  */
//TODO: merge into MessageAPI
object PrettyProduct {

  def product2String(
                      x: Product,
                      start: String = "(",
                      sep: String = ",",
                      end: String = ")",
                      indentFn: Int => String = _ => "",
                      recursion: Int = 0
                    ): String = {
    x match {
      case v: Literal[_, _] => v.toString
      case _ =>
        val strs = x.productIterator
          .map {
            case vv: PrettyProduct => vv.product2String(start, sep, end) //has verbose over
            case vv: Product => product2String(vv, start, sep, end, indentFn, recursion + 1)
            case vv@ _ => "" + vv
          }
          .map {
            str =>
              val indent = indentFn(recursion)
              indent + str
          }
        val nonVerbose = strs
          .mkString(x.productPrefix + start, sep, end)
        nonVerbose
    }
  }
}

trait PrettyProduct extends Product {

  abstract override def toString = this.product2String()

  def toString_\\\ = this.product2String(File.separator, File.separator, File.separator)
  def toString_/:/ = this.product2String("/", "/", "/")

  def product2String(
                      start: String = "(",
                      sep: String = ",",
                      end: String = ")"
                    ): String = {

    PrettyProduct.product2String(this, start, sep, end)
  }
}

trait VerboseProduct extends PrettyProduct with Verbose