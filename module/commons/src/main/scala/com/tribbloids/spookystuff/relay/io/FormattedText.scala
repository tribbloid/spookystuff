package com.tribbloids.spookystuff.relay.io

import com.tribbloids.spookystuff.commons.DSLUtils
import com.tribbloids.spookystuff.relay.{IR, TreeIR}
import pprint.PPrinter

import scala.language.implicitConversions

trait FormattedText {

  def useColor: Boolean
  lazy val basePrinter: PPrinter = {
    val base =
      if (useColor) PPrinter.Color
      else PPrinter.BlackWhite

    base.copy(defaultShowFieldNames = false)
  }

  def printer: PPrinter

  def zipRows[_IR <: IR](ir: TreeIR[?], childrenRows: Seq[String]): String

  case class TreeWriter[_IR <: IR](ir: _IR) {

    val _ir: _IR & TreeIR[?] = ir match {
      case v: _IR with TreeIR[_] =>
        v
      case _ =>
        throw new UnsupportedOperationException(s"IR type ${ir.getClass.getName} is not supported")
    }

    lazy val aggregated: String = {

      _ir.DepthFirstTransform
        .onLeaves[String] { v =>
          TreeIR.leaf(printer(v.body).toString())
        }
        .up[String, TreeIR.Leaf[String]] {
          case ir: TreeIR.Leaf[String] =>
            ir
          case ir: TreeIR.ListTree[String] =>
            val childrenRows = ir.children.map { v =>
              s"${v.body}"
            }
            val text = zipRows(ir, childrenRows)
            TreeIR.leaf(text)
          case ir: TreeIR.MapTree[_, _] =>
            val childrenRows = ir.repr.toSeq.map {
              case (k, v: TreeIR.Leaf[_]) =>
                if (ir.isSchemaless) {
                  s"$k -> ${v.body}"
                } else {
                  s"${v.body}"
                }
              case (k, v) =>
                s"$k -> $v (UNFOLDED)"
            }
            val text: String = zipRows(ir, childrenRows)
            TreeIR.leaf(text)
        }
        .execute
        .body
    }

    lazy val text: String = {
      aggregated
    }
  }

  def forValue[V](v: V): TreeWriter[TreeIR.Leaf[V]] = TreeWriter(TreeIR.leaf(v))
}

object FormattedText {

  case class Flat(
      open: String = "(",
      separator: String = ", ",
      close: String = ")",
      useColor: Boolean = false
  ) extends FormattedText {

    val (_indent, _maxWidth) = 0 -> Int.MaxValue

    override def printer: PPrinter = {
      basePrinter.copy(
        defaultWidth = _maxWidth,
        defaultIndent = _indent
      )
    }

    override def zipRows[_IR <: IR](ir: TreeIR[?], childrenRows: Seq[String]): String = {

      val enclosed = childrenRows.mkString(open, separator, close)
      s"${ir.rootTag}$enclosed"
    }
  }

  case class Tree(
      indent: Int = 2,
      maxWidth: Int = Int.MaxValue,
      useColor: Boolean = false
  ) extends FormattedText {

    override def printer: PPrinter = {
      basePrinter.copy(
        defaultWidth = maxWidth,
        defaultIndent = indent
      )
    }

    override def zipRows[_IR <: IR](ir: TreeIR[?], childrenRows: Seq[String]): String = {

      val indented = DSLUtils.indent(
        childrenRows.mkString(System.lineSeparator()),
        " " * indent
      )
      s"""
         |${ir.rootTag}
         |$indented
         |""".stripMargin.trim
    }
  }

  object Path_/:/ extends Flat("/", "/", "")

  object Path_\\\ extends Flat(System.lineSeparator(), System.lineSeparator(), "")

  object Color_2 extends Tree(maxWidth = 1, useColor = true)

  object NoColor_2 extends Tree(maxWidth = 1)

  implicit def toDefault(self: this.type): NoColor_2.type = NoColor_2
}
