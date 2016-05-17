package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.row.FetchedRow
import com.tribbloids.spookystuff.session.Session
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{DataType, UserDefinedType}

/**
 * Created by peng on 11/7/14.
 */
abstract class ActionLike extends TreeNode[ActionLike] with Product with Serializable {

  final def interpolate(pr: FetchedRow, context: SpookyContext): Option[this.type] = {
    val option = this.doInterpolate(pr, context)
    option.foreach{
      action =>
        action.injectFrom(this)
    }
    option
  }

  def doInterpolate(pageRow: FetchedRow, context: SpookyContext): Option[this.type] = Some(this)

  def injectFrom(same: ActionLike): Unit = {} //TODO: change to immutable pattern to avoid one Trace being used twice with different names

  final def injectTo(same: ActionLike): Unit = same.injectFrom(this)

  //used to determine if snapshot needs to be appended or if possible to be executed lazily
  final def hasOutput: Boolean = outputNames.nonEmpty

  def outputNames: Set[String]

  //the minimal equivalent action that can be put into backtrace
  def trunk: Option[this.type]

  def apply(session: Session): Seq[Fetched]
}

class ActionLikeUDT extends UserDefinedType[ActionLike] {
  override def sqlType: DataType = ???

  override def serialize(obj: Any): Any = ???

  override def userClass: Class[ActionLike] = classOf[ActionLike]

  override def deserialize(datum: Any): ActionLike = ???
}