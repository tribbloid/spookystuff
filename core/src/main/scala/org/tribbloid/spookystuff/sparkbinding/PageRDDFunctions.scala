package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.{Visit, Snapshot, ActionPlan, HtmlPage}
import java.io.Serializable
import scala.reflect.ClassTag

import java.util

//this is an implicit RDD view of Page
//all intermediate results after a transformation can be persisted to memory or disk, thus reusable.
//mimic sql keywords
class PageRDDFunctions(val self: RDD[HtmlPage]) {

  def as(alias: String): RDD[HtmlPage] = self.map{ _.modify(alias = alias) }

  def from(alias: String): RDD[HtmlPage] = self.filter{ _.alias == alias }

  def clearContext(): RDD[HtmlPage] = self.map{ _.modify(context = null) }

  def where(f: HtmlPage => Boolean) = self.filter(f)

  //TODO: just a map, this is the most abominable interface so far, will gradually evolve to resemble Spark SQL's select
  def select[T: ClassTag](f: HtmlPage => T): RDD[T] = self.map[T] { f(_) }

  def selectAll[T: ClassTag](f: HtmlPage => Seq[T]): RDD[T] = self.flatMap[T]( f(_) )

  def select(keyAndF: (String, HtmlPage => Serializable)*): RDD[util.Map[String, Serializable]] = self.map {

    page =>  page.asMap(keyAndF: _*)
  }

  def addToContext(keyAndF: (String, HtmlPage => Serializable)*): RDD[HtmlPage] = self.map {

    page => {
      val map = page.asMap(keyAndF: _*)
      val newPage = page.clone

      newPage.context.putAll(map)

      newPage
    }
  }

  def replaceIntoContext(keyAndF: (String, HtmlPage => Serializable)*): RDD[HtmlPage] = self.map {

    page => {
      val map = page.asMap(keyAndF: _*)
      page.modify(context = map)
    }
  }

  def slice(selector: String): RDD[HtmlPage] = self.flatMap(_.slice(selector))


  //if the page doesn't contain the selector it will throw an exception
  //pass all context to ActionPlans
  def linkFirst(selector: String): RDD[ActionPlan] = self.map{
    page => {

      val context = page.context
      new ActionPlan(context) + Visit(page.linkFirst(selector))
    }
  }

  //will drop pages that doesn't contain the selector
  def linkAll(selector: String): RDD[ActionPlan] = self.flatMap{
    page => {

      val context = page.context
      page.linkAll(selector).map {
        new ActionPlan(context) + Visit(_)
      }
    }
  }

  def crawlFirst(selector: String): RDD[HtmlPage] = new ActionPlanRDDFunctions(this.linkFirst(selector)) >!<

  def fork(selector: String): RDD[HtmlPage] = new ActionPlanRDDFunctions(this.linkAll(selector)) >!<

//  def crawlFirstIf(selector: String)(condition: HtmlPage => Boolean): RDD[HtmlPage] = self.map{
//    page => {
//
//      if (condition(page) == true)
//      {
//        val context = page.context
//        page.links(selector).map {
//          new ActionPlan(context) + Visit(_)
//        }
//      }
//      else
//      {
//
//      }
//    }
//  }

  //really complex but what option do I have
  //these are slow because backward lookup hasn't been implemented yet.
  //its an optimization problem:
  //whether we split the old pages, crawl the disambiguation part, >!<+ (lookup only the new plan from old pages) to new pages, and merge with the old pages
  //or we crawl the disambiguation part, merge with old ActionPlans and >!<+ (lookup all plans from old pages) to get the new pages?
  //I prefer the first one, potentially produce smaller footage.
  //TODO: there is no repartitioning in the process, may cause unbalanced execution
  def crawlFirstIf(selector: String)(f: HtmlPage => Boolean): RDD[HtmlPage] = {
    val groupedPageRDD = self.map{ page => (f(page), page) }
    val falsePageRDD = groupedPageRDD.filter(_._1 == false).map(_._2)
    val truePageRDD = groupedPageRDD.filter(_._1 == true).map(_._2)
    val newPageRDD = new PageRDDFunctions(truePageRDD).crawlFirst(selector)
    newPageRDD.union(falsePageRDD)
  }

  //really complex but what option do I have
  def forkIf(selector: String)(f: HtmlPage => Boolean): RDD[HtmlPage] = {
    val groupedPageRDD = self.map{ page => (f(page), page) }
    val falsePageRDD = groupedPageRDD.filter(_._1 == false).map(_._2)
    val truePageRDD = groupedPageRDD.filter(_._1 == true).map(_._2)
    val newPageRDD = new PageRDDFunctions(truePageRDD).fork(selector)
    newPageRDD.union(falsePageRDD)
  }
  //----------------------------------------------
  //save should move to Actions
}