package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.sparkbinding.{StringRDDFunctions, ActionChainRDDFunctions, PageRDDFunctions}
import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{ActionChain, Page}
import org.apache.spark.SparkContext
import java.util
import scala.collection.JavaConversions._

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

object SpookyContext {
  implicit def pageRDDToItsFunctions(rdd: RDD[Page]) = new PageRDDFunctions(rdd)

  implicit def actionChainRDDToItsFunctions(rdd: RDD[ActionChain]) = new ActionChainRDDFunctions(rdd)

  implicit def stringRDDToItsFunctions(rdd: RDD[String]) = new StringRDDFunctions(rdd)



  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  implicit def stringRDDToActionChainRDD(rdd: RDD[String]): RDD[ActionChain] = rdd.map{
    str => {
      val context = new util.HashMap[String,String]
      context.put("_", str)
      new ActionChain(context)
    }
  }

  implicit def stringRDDToActionChainRDDFunctions(rdd: RDD[String]) = new ActionChainRDDFunctions(stringRDDToActionChainRDD(rdd))



  implicit def mapRDDToActionChainRDD(rdd: RDD[Map[String,String]]) = rdd.map{
    map => {
      new ActionChain(map)
    }
  }

  implicit def mapRDDToActionChainRDDFunctions(rdd: RDD[Map[String,String]]) = new ActionChainRDDFunctions(mapRDDToActionChainRDD(rdd))


}
