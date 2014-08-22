package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{Page, ActionPlan, Action}
import org.tribbloid.spookystuff.factory.PageBuilder
import scala.collection.mutable.ArrayBuffer
import java.util
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._

/**
 * An implicit wrapper class for RDD[ActionPlan], representing a set of similar instruction sequences for a
 * web browser to reach all pages being scraped.
 *
 * Can be constructed from either RDD[String] or RDD[Map] as context/metadata:
 * @example {{{
 *  val context = sc.parallelize( seq("Metallica", "Megadeth") )
 *  val plans = context +> Visit("http://www.youtube.com/user/#{_}TV")
 *  }}}
 * @example {{{
 *  val context = sc.parallelize( seq(Map("language"->"en","topic"->"Bat"), Map("language"->"de","topic"->"Fledertiere")) )
 *  val plans = context +> Visit("http://#{language}.wikipedia.org/wiki/#{topic}")
 *  }}}
 * Created by peng on 06/06/14, D-day!
 */
class ActionPlanRDDFunctions(val self: RDD[ActionPlan]) {

  /**
   * append an action
   * @param action any object that inherits org.tribbloid.spookystuff.entity.Action
   * @return new RDD[ActionPlan]
   */
  def +>(action: Action): RDD[ActionPlan] = self.map {
    _ + (action)
  }

  /**
   * append a series of actions
   * equivalent to ... +> action1 +> action2 +>...actionN, where action{1...N} are elements of actions
   * @param actions = Seq(action1, action2,...)
   * @return new RDD[ActionPlan]
   */
  def +>(actions: Seq[Action]): RDD[ActionPlan] = self.map {
    _ + (actions)
  }

  /**
   * append all actions in another ActionPlan
   * will NOT merge context
   * this may become an option in the next release
   * @param ap another ActionPlan
   * @return new RDD[ActionPlan]
   */
  def +>(ap: ActionPlan): RDD[ActionPlan] = self.map {
    _ + ap
  }

  /**
   * append a set of actions or ActionPlans to be executed in parallel
   * each old ActionPlan yields N new ActionPlans, where N is the size of the new set
   * results in a cartesian product of old and new set.
   * @param actions a set of Actions/Sequences to be appended and executed in parallel
   *                 can be Seq[Action], Seq[ Seq[Action] ] or Seq[ActionPlan], or any of their combinations.
   *                 CANNOT be RDD[ActionPlan], but this may become an option in the next release
   * @return new RDD[ActionPlan], of which size = [size of old RDD] * [size of actions]
   */
  def +*>(actions: Seq[_]): RDD[ActionPlan] = self.flatMap {
    old => {
      val results: ArrayBuffer[ActionPlan] = ArrayBuffer()

      actions.foreach {
        action => action match {
          case a: Action => results += (old + a)
          case sa: Seq[Action] => results += (old + sa)
          case ap: ActionPlan => results += (old + ap)
          case _ => throw new UnsupportedOperationException("Can only append Action, Seq[Action] or ActionPlan")
        }
      }

      results
    }
  }

  /**
   * parallel execution in browser(s) to yield a set of web pages
   * each ActionPlan may yield several pages in a row, depending on the number of Export(s) in it
   * @return RDD[Page] as results of execution
   */
  def !==(): RDD[Page] = {
    val hConfWrapper = self.context.broadcast(new SerializableWritable(self.context.hadoopConfiguration))

    self.flatMap {
      ap =>{
        var pages = PageBuilder.resolve(ap.actions: _*)(hConfWrapper.value.value)
        //has to use deep copy, one to many mapping and context may be modified later
        pages = pages.map { _.copy(context = new util.LinkedHashMap(ap.context)) }
        pages
      }
    }
  }

  //there is no repartitioning in the process, may cause unbalanced execution, but apparently groupByKey will do it automatically
  //TODO: this definitely need some logging to let us know how many actual resolves.
  /**
   * smart execution: group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
   * reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
   * recommended for most cases, mandatory for RDD[ActionPlan] with high duplicate factor, only use !() if you are sure that duplicate doesn't exist.
   * @return RDD[Page] as results of execution
   */
  def !><(): RDD[Page] = {
    val hConfWrapper = self.context.broadcast(new SerializableWritable(self.context.hadoopConfiguration))

    val squashedPlanRDD = self.map{ ap => (ap.actions, ap.context) }.groupByKey()

    val squashedPageRDD = squashedPlanRDD.flatMap { tuple => PageBuilder.resolve(tuple._1: _*)(hConfWrapper.value.value).map{ (_, tuple._2) } }

    return squashedPageRDD.flatMap {
      tuple => tuple._2.map {
        cc => tuple._1.copy(context = new util.LinkedHashMap(cc))
      }
    }
  }

  //find non-expired page from old results, if not found, execute
  // this will be handy if all pages are dumped into a history RDD/Spark stream
  //this is really hard, if only one snapshot is not found, the entire plan has to be executed again
  //  def !+(old: RDD[Page])
}