package org.tribbloid.spookystuff

import scala.language.implicitConversions

/**
* Created by peng on 9/18/14.
*/
package object actions {

//  type SerializableCookie = Cookie with Serializable

  implicit def traceSetFunctions(traces: Set[Trace]): TraceSetFunctions = new TraceSetFunctions(traces)

  implicit def traceSet(action: Action): Set[Trace] = Set(Trace(Seq(action)))

  implicit def traceSetFunctions(action: Action): TraceSetFunctions = new TraceSetFunctions(Set(Trace(Seq(action))))

//  implicit def stringValue(str: String): Value[String] = Value(str)
}