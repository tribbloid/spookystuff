//package com.tribbloids.spookystuff.graph
//
//import com.tribbloids.spookystuff.graph.DirectNode.DirectLink
//
//case class DirectNode[D <: Domain](
//    node: Element.Node[D]
//) {
//
//  // both of these can only be set once
//  protected var _upstream: Seq[DirectLink[D]] = _
//  protected var _downstream: Seq[DirectLink[D]] = _
//
//  def isSet: Boolean = {
//    _upstream != null && _downstream != null
//  }
//
//  def isSetRecursively: Boolean = {
//    isSet &&
//    _upstream.forall(_._2.isSetRecursively) &&
//    _downstream.forall(_._2.isSetRecursively)
//  }
//
//  def upstream = Option(_upstream).get
//  def downstream = Option(_downstream).get
//
//  def setUpstream(v: Seq[DirectLink[D]]) = {
//    assert(_upstream != null)
//    _upstream = v
//  }
//
//  def setDownstream(v: Seq[DirectLink[D]]) = {
//    assert(_downstream != null)
//    _downstream = v
//  }
//}
//
//object DirectNode {
//
//  type DirectLink[D] = (Element.Edge[D], DirectNode[D])
//}
