package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.row.AgentContext
import org.apache.spark.sql.types.SQLUserDefinedType

object Node {

  object Unrecognisable extends Node {
//    override def uri: String = ""
    override def findAll(selector: DocSelector): Seq[Node] = Seq.empty
    override def findAllWithSiblings(selector: DocSelector, range: Range): Seq[Siblings[Node]] =
      Seq.empty
    override def children(selector: DocSelector): Seq[Node] = Seq.empty
    override def childrenWithSiblings(selector: DocSelector, range: Range): Seq[Siblings[Node]] =
      Seq.empty
    override def code: Option[String] = None
    override def formattedCode: Option[String] = None
    override def text: Option[String] = None
    override def ownText: Option[String] = None
    override def boilerPipe: Option[String] = None
    override def breadcrumb: Option[Seq[String]] = None
    override def allAttr: Option[Map[String, String]] = None
    override def attr(attr: String, noEmpty: Boolean): Option[String] = None
    override def href: Option[String] = None
    override def src: Option[String] = None
  }

  implicit class _batchView[+T <: Node](override val nodeSeq: Seq[T]) extends ManyNodes[T] {

    def only: T = {

      require(nodeSeq.size == 1, s"expected 1 node, but got ${nodeSeq.size}")
      nodeSeq.head
    }
  }

  /**
    * mixin type that enabled methods that uses [[com.tribbloids.spookystuff.row.AgentContext]] & [[SpookyContext]] as
    * part of the context, like "save"
    */
  trait AgenticMixin {
    self: Node =>

    def agentState: AgentContext

  }

  type Agentic[T <: Node] = T & AgenticMixin
}

@SQLUserDefinedType(udt = classOf[UnstructuredUDT])
trait Node extends NodeContainer[Node] {

  final def findFirstWithSiblings(selector: DocSelector, range: Range): Option[Siblings[Node]] =
    findAllWithSiblings(selector, range).headOption

  final def child(selector: DocSelector): Option[Node] =
    children(selector).headOption

  def childrenWithSiblings(
      selector: DocSelector,
      range: Range
  ): Seq[Siblings[Node]]

  final def childWithSiblings(selector: DocSelector, range: Range): Option[Siblings[Node]] =
    findAllWithSiblings(selector, range).headOption

  def code: Option[String]

  def formattedCode: Option[String]

  def text: Option[String]

  def ownText: Option[String]

  def boilerPipe: Option[String]

  def breadcrumb: Option[Seq[String]]

  def allAttr: Option[Map[String, String]]

  def attr(attr: String, noEmpty: Boolean = true): Option[String]

  def href: Option[String]

  def src: Option[String]
}
