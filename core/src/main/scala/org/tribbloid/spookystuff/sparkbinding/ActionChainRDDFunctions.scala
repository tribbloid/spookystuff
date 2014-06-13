package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{PageBuilder, Page, ActionChain, Action}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by peng on 06/06/14.
 */
class ActionChainRDDFunctions(val self: RDD[ActionChain]) {

  def +> (actions: Action*): RDD[ActionChain] = self.map{ _ + (actions: _* ) }

  //will remove context of the parameter! cannot merge two context as they may have conflict keys
  def +> (ac: ActionChain): RDD[ActionChain] = self.map{ _ + ac }


  //one to many: cartesian product-ish
  def *> (actions: Action*): RDD[ActionChain] = self.flatMap{
    old => {
      val results: ArrayBuffer[ActionChain] = ArrayBuffer()

      actions.foreach{
        action => {
          results += (old + action)
        }
      }

      results
    }
  }

  //TODO: unfortunately the name of this operator has to be different to avoid TYPE ERASURE
  //will remove context of the parameter! cannot merge two context as they may have conflict keys
  def **> (acs: ActionChain*): RDD[ActionChain] = self.flatMap{
    old => {
      val results: ArrayBuffer[ActionChain] = ArrayBuffer()

      acs.foreach{
        ac => {
          results += (old + ac)
        }
      }

      results
    }
  }

  //  //resolveFinal
    def !(): RDD[Page] = self.map{ PageBuilder.resolveFinal(_) }

  //  //resolve
    def !!!(): RDD[Page] = self.flatMap{ PageBuilder.resolve(_) }
}