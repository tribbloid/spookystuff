package org.tribbloid.spookystuff.expressions

/**
 * Created by peng on 8/31/14.
 */
sealed abstract class JoinType extends Serializable with Product

//trait LeftLink extends JoinType //Join yield at least 1 PageRow that may have empty action chain, now assuming always happen
case object Inner extends JoinType
case object LeftOuter extends JoinType //Flatten yield at least 1 PageRow that may have no page
case object Replace extends JoinType //keep the original pages if action chain is empty
case object Append extends JoinType //always keep the original pages
case object Merge extends JoinType