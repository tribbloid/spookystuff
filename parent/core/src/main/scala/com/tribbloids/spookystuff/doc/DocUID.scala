package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions._

/**
  * Created by peng on 04/06/14.
  */
//use to genterate a lookup key for each page so
@SerialVersionUID(612503421395L)
case class DocUID(
    backtrace: Trace,
    output: Export,
    //                    sessionStartTime: Long,
    blockIndex: Int = 0,
    blockSize: Int = 1
)( //number of pages in a block output,
  val name: String = Option(output).map(_.name).orNull) {}
