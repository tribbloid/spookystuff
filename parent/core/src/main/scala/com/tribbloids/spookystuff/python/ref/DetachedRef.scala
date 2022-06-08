package com.tribbloids.spookystuff.python.ref

import com.tribbloids.spookystuff.python.PyConverter

case class DetachedRef(
    override val createOpt: Option[String],
    override val referenceOpt: Option[String],
    override val dependencies: Seq[PyRef],
    override val converter: PyConverter
) extends PyRef {

  override def lzy = false
}
