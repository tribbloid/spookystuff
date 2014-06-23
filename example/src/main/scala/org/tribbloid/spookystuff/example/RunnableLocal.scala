package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by peng on 22/06/14.
  */
trait RunnableLocal {

   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("MoreLinkedIn")
     conf.setMaster("local[*]")
     val sc = new SparkContext(conf)

     doMain()

     sc.stop()
   }

   def doMain()
 }
