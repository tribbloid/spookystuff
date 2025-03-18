package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.HasTrace.StateChangeTag

object MayExport {

  trait Named extends MayExport {
    self: StateChangeTag =>

    private var _nameOvrd: String = _

    protected def originalName: String = this.productPrefix

    def name: String = Option(_nameOvrd).getOrElse(originalName)
    def name_=(v: String): Unit = _nameOvrd = v
  }

  implicit class _Ops[T <: Named](self: T) {

    def as(name: String): T = {
      val copied = self.deepCopy()
      copied.name = name
      copied
    }

    final def ~(name: String): T = as(name)
  }
}

trait MayExport extends Action {
  self: StateChangeTag =>

//  def outputName: String = this.productPrefix
//
//  final override def outputNames: Set[String] = Set(outputName)
}
