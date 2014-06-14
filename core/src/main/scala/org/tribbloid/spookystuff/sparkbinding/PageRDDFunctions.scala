package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{ActionPlan, Page}
import java.io.Serializable
import scala.reflect.ClassTag

import java.util

//this is an implicit RDD view of Page
//all intermediate results after a transformation can be persisted to memory or disk, thus reusable.
//mimic sql keywords
class PageRDDFunctions(val self: RDD[Page]) {

  def as(alias: String): RDD[Page] = self.map{ _.modify(alias = alias) }

  def from(alias: String): RDD[Page] = self.filter{ _.alias == alias }

  def select[T: ClassTag](f: Page => T): RDD[T] = self.map[T] { f(_) }

  def selectAsMap(keyAndF: (String, Page => _ >: Serializable)*): RDD[util.Map[String, Serializable]] = self.map {

    page => {
      val result: util.Map[String, Serializable] = new util.HashMap()

      keyAndF.foreach{
        fEntity => {
          val value = fEntity._2(page)
          result.put(fEntity._1, value.asInstanceOf[Serializable])
        }
      }

      result
    }
  }

  def where(f: Page => Boolean) = self.filter(f)

//  def link(selector: String, limit: Int = 1): RDD[ActionChain] = self.flatMap{
//    page => {
//      page.allLinks(selector).slice(0,limit)
//    }
//  }

//  def slice(): RDD[Page]

  //----------------------------------------------
  //
  //  def save(fileName: String, usePattern: Boolean = false) {
  //
  //  }
}