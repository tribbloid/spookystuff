package org.tribbloid.spookystuff

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.tribbloid.spookystuff.sparkbinding.{StringRDDFunctions, ActionPlanRDDFunctions, PageRDDFunctions}
import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{ActionPlan, Page}
import java.io.Serializable
import java.util

/**
 * Created by peng on 12/06/14.
 */
//class SpookyContext(val sc: SparkContext) {
//
//}

object SpookyContext {

  implicit def pageRDDToItsFunctions(rdd: RDD[Page]) = new PageRDDFunctions(rdd)

  implicit def ActionPlanRDDToItsFunctions(rdd: RDD[ActionPlan]) = new ActionPlanRDDFunctions(rdd)

  implicit def stringRDDToItsFunctions(rdd: RDD[String]) = new StringRDDFunctions(rdd)

  //these are the entry points of SpookyStuff starting from a common RDD of strings or maps
  implicit def stringRDDToActionPlanRDD(rdd: RDD[String]): RDD[ActionPlan] = rdd.map{
    str => {
      val context = new util.HashMap[String,Serializable]
      context.put("_", str.asInstanceOf[Serializable])
      new ActionPlan(context)
    }
  }

  implicit def stringRDDToActionPlanRDDFunctions(rdd: RDD[String]) = new ActionPlanRDDFunctions(stringRDDToActionPlanRDD(rdd))



  implicit def mapRDDToActionPlanRDD(rdd: RDD[util.Map[String,Serializable]]) = rdd.map{
    map => {
      new ActionPlan(map)
    }
  }

  implicit def mapRDDToActionPlanRDDFunctions(rdd: RDD[util.Map[String,Serializable]]) = new ActionPlanRDDFunctions(mapRDDToActionPlanRDD(rdd))

  implicit def pageRDDToActionPlanRDD(rdd: RDD[Page]) = rdd.map{
    page => {
      new ActionPlan(page.context)
    }
  }

  implicit def pageRDDToActionPlanRDDFunctions(rdd: RDD[Page]) = new ActionPlanRDDFunctions(pageRDDToActionPlanRDD(rdd))
}
