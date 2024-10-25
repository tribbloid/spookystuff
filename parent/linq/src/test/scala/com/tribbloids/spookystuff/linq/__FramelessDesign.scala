package com.tribbloids.spookystuff.linq

object __FramelessDesign {

  /**
    * most implementation in this package should be removed once the main PR is integrated:
    *
    * https://github.com/typelevel/frameless/issues/777
    *
    * shapeless is abandoned for Scala 3 (shapeless-3 is nominal only), usage should be minimised except components with
    * known successors (HList, Record, Poly)
    *
    * but in practice, it is very likely that a compatibility library will be published before Apache Spark upgrade to
    * Scala 3 (six.scala?), most implementation in this package depends on such hypothesis
    *
    * in worst case we have to hard-fork shapeless to become six.scala, but this requires community support
    */
}
