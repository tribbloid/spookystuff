//package org.tribbloid.spookystuff.expressions
//
//import org.tribbloid.spookystuff.actions.Trace
//import org.tribbloid.spookystuff.utils._
//
///**
// * Created by peng on 9/12/14.
// */
////abstract class TraceEncoder[T] extends (Trace => T) with Serializable with Product
//
//
//
//case object HierarchicalUrnEncoder extends TraceEncoder[String] {
//
//  override def apply(trace: Trace): String = {
//
//    val actionStrs = trace.self.map(_.toString)
//
//    val actionConcat = if (actionStrs.size > 4) {
//      val oneTwoThree = actionStrs.slice(0,3)
//      val last = actionStrs.last
//      val omitted = "/"+(trace.self.length-4).toString+"more"+"/"
//
//      oneTwoThree.mkString("/")+omitted+last
//    }
//    else actionStrs.mkString("/")
//
//    val hash = "-"+trace.hashCode
//
//    canonizeUrn(actionConcat + hash)
//  }
//}