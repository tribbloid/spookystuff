package com.tribbloids.spookystuff.doc

trait NodeContainer[+T <: Node] extends Serializable {

  def findAll(selector: DocSelector): Seq[Node]

  def findAllWithSiblings(
      selector: DocSelector,
      range: Range
  ): Seq[Siblings[Node]]

  def children(selector: DocSelector): Seq[Node]

  // ---

  final def select(selector: DocSelector): Seq[Node] = findAll(selector)

  final def \(selector: DocSelector): Seq[Node] = findAll(selector)

  final def findOnly(selector: DocSelector): Node = findAll(selector).only

  final def findFirst(selector: DocSelector): Option[Node] =
    findAll(selector).headOption
}
