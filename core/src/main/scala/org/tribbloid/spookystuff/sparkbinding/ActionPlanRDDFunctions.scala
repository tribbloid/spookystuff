package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{PageBuilder, HtmlPage, ActionPlan, Action}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._

/**
 * Created by peng on 06/06/14.
 */
class ActionPlanRDDFunctions(val self: RDD[ActionPlan]) {

  def +>(actions: Action*): RDD[ActionPlan] = self.map {
    _ + (actions: _*)
  }

  //will remove context of the parameter! cannot merge two context as they may have conflict keys
  def +>(ap: ActionPlan): RDD[ActionPlan] = self.map {
    _ + ap
  }


  //one to many: cartesian product-ish
  def *>(actions: Action*): RDD[ActionPlan] = self.flatMap {
    old => {
      val results: ArrayBuffer[ActionPlan] = ArrayBuffer()

      actions.foreach {
        action => {
          results += (old + action)
        }
      }

      results
    }
  }

  //TODO: unfortunately the name of this operator has to be different to avoid TYPE ERASURE
  //will remove context of the parameter! cannot merge two context as they may have conflict keys
  def **>(aps: ActionPlan*): RDD[ActionPlan] = self.flatMap {
    old => {
      val results: ArrayBuffer[ActionPlan] = ArrayBuffer()

      aps.foreach {
        ac => {
          results += (old + ac)
        }
      }

      results
    }
  }

  //  //execute
  def !!!(): RDD[HtmlPage] = self.flatMap {
    _ !!!
  }

  //  //only execute interactions and extract the final stage
  def !(): RDD[HtmlPage] = self.map {
    _ !
  }

  //smart execution: will merge identical plan, execute, and split to match the original context
  //this is useful to handle diamond links (A->B,A->C,B->D,C->D)
  //be careful this step is complex and may take longer than plain execution if unoptimized
  //there is no repartitioning in the process, may cause unbalanced execution, but apparently groupByKey will do it automatically
  //TODO: this definitely need some logging to let us know how many actual resolves.
  def >!!!<(): RDD[HtmlPage] = {
    val squashedPlanRDD = self.map{ ap => (ap.actions, ap.context) }.groupByKey()

    val squashedPageRDD = squashedPlanRDD.flatMap { tuple => PageBuilder.resolve(tuple._1: _*).map{ (_, tuple._2) } }

    return squashedPageRDD.flatMap {
      tuple => tuple._2.map {
        cc => tuple._1.modify(context = cc)
      }
    }
  }

  def >!<(): RDD[HtmlPage] = {
    val squashedPlanRDD = self.map{ ap => (ap.interactions, ap.context) }.groupByKey()

    val squashedPageRDD = squashedPlanRDD.map { tuple => ( PageBuilder.resolveFinal(tuple._1: _*), tuple._2) }

    return squashedPageRDD.flatMap {
      tuple => tuple._2.map {
        cc => tuple._1.modify(context = cc)
      }
    }
  }

//  //find non-expired page from old results, if not found, execute
  // this will be really handy if all pages are dumped into a history RDD/Spark stream
//  def !+(old: RDD[Page])
//
//  //this is really hard, if only one snapshot is not found, the entire plan has to be executed again
//  def !!!+(old: RDD[Page])
}