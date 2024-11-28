package com.tribbloids.spookystuff.actions

trait MayExport extends Action {

  def as(name: String): Named.Explicitly[this.type] = {
    assert(name != null)

    Named.Explicitly(this, name)
  }

  final def ~(name: String): Named.Explicitly[MayExport.this.type] = as(name)
}
