package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.sparkbinding.{StringRDDFunctions, ActionPlanRDDFunctions, PageRDDFunctions}
import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{ActionPlan, HtmlPage}
import java.io.Serializable
import java.util
import scala.collection.JavaConversions._

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

object SpookyContext {
  implicit def pageRDDToItsFunctions(rdd: RDD[HtmlPage]) = new PageRDDFunctions(rdd)

  implicit def actionChainRDDToItsFunctions(rdd: RDD[ActionPlan]) = new ActionPlanRDDFunctions(rdd)

  implicit def stringRDDToItsFunctions(rdd: RDD[String]) = new StringRDDFunctions(rdd)

  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  implicit def stringRDDToActionChainRDD(rdd: RDD[String]): RDD[ActionPlan] = rdd.map{
    str => {
      val context = new util.HashMap[String,Serializable]
      context.put("_", str.asInstanceOf[Serializable])
      new ActionPlan(context)
    }
  }

  implicit def stringRDDToActionChainRDDFunctions(rdd: RDD[String]) = new ActionPlanRDDFunctions(stringRDDToActionChainRDD(rdd))



  implicit def mapRDDToActionChainRDD(rdd: RDD[Map[String,Serializable]]) = rdd.map{
    map => {
      new ActionPlan(map)
    }
  }

  implicit def mapRDDToActionChainRDDFunctions(rdd: RDD[Map[String,Serializable]]) = new ActionPlanRDDFunctions(mapRDDToActionChainRDD(rdd))

}
