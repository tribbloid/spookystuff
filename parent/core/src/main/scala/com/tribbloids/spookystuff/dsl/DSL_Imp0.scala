package com.tribbloids.spookystuff.dsl

import scala.language.implicitConversions

trait DSL_Imp0 extends DSL_Imp1 {

//  implicit class StrContextOps(val strC: StringContext) extends Serializable {
//
//    def x(parts: Col[String]*): Interpolate = Interpolate(strC.parts, parts.map(_.ex))
//
//    def CSS(parts: Col[String]*): GenExtractor[FR, Elements[Unstructured]] =
//      GetOnlyDocExpr.andMap(_.root).findAll(strC.s(parts: _*))
//    def S(parts: Col[String]*): GenExtractor[FR, Elements[Unstructured]] = CSS(parts: _*)
//
//    def CSS_*(parts: Col[String]*) = GetAllDocsExpr.findAll(strC.s(parts: _*))
//    def S_*(parts: Col[String]*) = CSS_*(parts: _*)
//
//    def A(parts: Col[String]*): GenExtractor[FR, Elements[Unstructured]] = 'A.findAll(strC.s(parts: _*))
//  }
}