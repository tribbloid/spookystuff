package com.tribbloids.spookystuff.actions

trait Named extends Action {

  var nameOpt: Option[String] = None
  def name: String = nameOpt.getOrElse(this.toString)

  def as(name: Symbol): this.type = {
    assert(name != null)

    this.nameOpt = Some(name.name)
    this
  }

  final def ~(name: Symbol): this.type = as(name)

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this.nameOpt = same.asInstanceOf[Named].nameOpt
  }
}
