package org.tribbloid.spookystuff.entity

/**
 * Created by peng on 8/29/14.
 */
abstract class SerializableClonable extends Serializable with Cloneable {

  override def clone() = super.clone()
}
