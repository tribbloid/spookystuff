//package com.tribbloids.spookystuff.dsl
//
///**
//  * Created by peng on 8/31/14.
//  */
////TODO: use Spark JoinType
//sealed abstract class OneToMany() extends Serializable with Product
//
//object OneToMany {
//
//  // trait LeftLink extends JoinType //Join yield at least 1 PageRow that may have empty action chain, now assuming always happen
//  case object Inner extends OneToMany()
//
//  case object Outer extends OneToMany() // Flatten yield at least 1 PageRow that may have no page
//  // case object Overwrite extends JoinType //keep the original pages if new trace is empty
//  // case object Append extends JoinType //always keep the original traces
//
//  // sealed class MergeStrategy(val f: (Any, Any) => Any)
//  //
//  // case object Deduplicate extends MergeStrategy(
//  //  (a,b) => {
//  //    if (a == b) a
//  //    else throw new UnsupportedOperationException("merge conflict")
//  //  }
//  // )
//  //
//  // case object First extends MergeStrategy((a,b) => a) //keep the original pages if action chain is empty
//  // case object Last extends MergeStrategy((a,b) => b) //always keep the original pages
//  // case object Error extends MergeStrategy(throw new UnsupportedOperationException("merge conflict"))
//  // case object Concat extends MergeStrategy((a,b) => a.asInstanceOf[Iterable[_]] ++ b.asInstanceOf[IterableOnce[_]])
//
//  val default: OneToMany = OneToMany.Outer
//}
