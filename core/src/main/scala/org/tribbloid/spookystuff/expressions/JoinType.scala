package org.tribbloid.spookystuff.expressions

/**
 * Created by peng on 8/31/14.
 */

sealed trait JoinType extends Serializable with Product

//trait LeftLink extends JoinType //Join yield at least 1 PageRow that may have empty action chain, now assuming always happen
case object Inner extends JoinType
case object LeftOuter extends JoinType //Flatten yield at least 1 PageRow that may have no page
case object Replace extends JoinType //keep the original pages if action chain is empty
case object Append extends JoinType //always keep the original pages
case object Merge extends JoinType

sealed class MergeStrategy(val f: (Any, Any) => Any)

case object Deduplicate extends MergeStrategy(
  (a,b) => {
    if (a == b) a
    else throw new UnsupportedOperationException("merge conflict")
  }
)

case object First extends MergeStrategy((a,b) => a) //keep the original pages if action chain is empty
case object Last extends MergeStrategy((a,b) => b) //always keep the original pages
case object Error extends MergeStrategy(throw new UnsupportedOperationException("merge conflict"))
case object Concat extends MergeStrategy((a,b) => a.asInstanceOf[Traversable[_]] ++ b.asInstanceOf[TraversableOnce[_]])