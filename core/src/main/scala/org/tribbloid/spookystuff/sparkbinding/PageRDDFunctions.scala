package org.tribbloid.spookystuff.sparkbinding

import java.io.Serializable

import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.Conf
import org.tribbloid.spookystuff.entity._
import java.util

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.factory.PageBuilder

import scala.collection.mutable.ArrayBuffer

//this is an implicit RDD view of Page
//all intermediate results after a transformation can be persisted to memory or disk, thus reusable.
//mimic sql keywords
class PageRDDFunctions(val self: RDD[Page]) {

  def as(alias: String): RDD[Page] = self.map{ _.copy(alias = alias) }

  def from(alias: String): RDD[Page] = self.filter{ _.alias == alias }

  def clearContext(): RDD[Page] = self.map{ _.copy(context = null) }

  //TODO: this is the most abominable interface so far, will gradually evolve to resemble Spark SQL's select
  def selectInto(keyAndF: (String, Page => Serializable)*): RDD[Page] = self.map {

    page => {
      val map = page.extractPropertiesAsMap(keyAndF: _*)

      //always replace old key-value pairs with new ones, old ones are flushed out
      val newContext = new util.HashMap[String,Serializable](page.context)
      newContext.putAll(map)

      page.copy(context = newContext)
    }
  }

  //reshape the organization of context.
  def select(keyAndF: (String, Page => Serializable)*): RDD[Page] = self.map {

    page => {
      val map = page.extractPropertiesAsMap(keyAndF: _*)
      page.copy(context = map)
    }
  }

  //this is a lazy transformation, use it to save overhead for rescheduling.
  def saveAs(fileName: String = "#{resolved-url}", dir: String = Conf.savePagePath, overwrite: Boolean = false): RDD[Page] = self.map {
    val hConfWrapper = self.context.broadcast(new SerializableWritable(self.context.hadoopConfiguration))

    page => {
      val name = page.save(fileName, dir, overwrite)(hConfWrapper.value.value)

      page.copy(savePath = name)
    }
  }

  //this is an action enforced to be executed, save to whatever (HDFS,S3,local disk) and return a list file paths
  def dump(fileName: String = "#{resolved-url}", dir: String = Conf.savePagePath, overwrite: Boolean = false): Array[String] = {
    val hConfWrapper = self.context.broadcast(new SerializableWritable(self.context.hadoopConfiguration))

    self.map(page => page.save(fileName, dir, overwrite)(hConfWrapper.value.value)).collect()
  }

  //pass all context to ActionPlans
  //SUGGESTION: all transformations that generates an RDD[ActionPlan] should use operator

  //ignore pages that doesn't contain the selector
  def visit(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[ActionPlan] = self.flatMap{
    page => {

      val context = page.context
      page.attr(selector,attr,limit).map {
        str => new ActionPlan(context, Visit(str))
      }
    }
  }

  //yield null for pages that doesn't contain the selector
  def leftVisit(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[ActionPlan] = self.flatMap{
    page => {

      val context = page.context
      var results = page.attr(selector,attr,limit).map {
        str => new ActionPlan(context, Visit(str))
      }
      if (results.size==0) results = results.:+(new EmptyActionPlan(context))
      results
    }
  }

  //why anybody want this as ActionPlan? private
  private def wget(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[ActionPlan] = self.flatMap{
    page => {

      val context = page.context
      page.attr(selector,attr,limit).map {
        str => new ActionPlan(context, Wget(str))
      }
    }
  }

  private def leftWget(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[ActionPlan] = self.flatMap{
    page => {

      val context = page.context
      var results = page.attr(selector,attr,limit).map {
        str => new ActionPlan(context, Wget(str))
      }
      if (results.size==0) results = results.:+(new ActionPlan(context) + Wget(null))
      results
    }
  }

  //inner join
  def join(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.visit(selector, limit, attr) !><

  def wgetJoin(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.wget(selector, limit, attr) !><

  def leftJoin(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.leftVisit(selector, limit, attr) !><

  def wgetLeftJoin(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.leftWget(selector, limit, attr) !><

  def joinBySlice(selector: String, as: String = null, limit: Int = Conf.fetchLimit): RDD[Page] =
    self.flatMap(_.slice(selector, as, limit))

  def leftJoinBySlice(selector: String, as: String = null, limit: Int = Conf.fetchLimit): RDD[Page] =
    self.flatMap {
      page => {

        val results = page.slice(selector, as, limit)
        if (results.size==0) Seq(PageBuilder.emptyPage.copy(context = page.context))
        else results
      }
    }

  //slower than nested action and wgetJoinByPagination
  //attr is always "abs:href"
  def insertPagination(selector: String, limit: Int = Conf.fetchLimit): RDD[Page] = self.flatMap {
    page => {
      val results = ArrayBuffer[Page](page)

      var currentPage = page
      var i = 0
      while (currentPage.attrExist(selector,"abs:href") && i< limit) {
        i = i+1
        val nextUrl = currentPage.href1(selector) //not null because already validated
        currentPage = PageBuilder.resolve(Visit(nextUrl))(0)
        results.+=(currentPage.copy(context = page.context))
      }

      results
    }
  }

  //TODO: to save time it should merge urls, find all pages and split by context.
  def wgetInsertPagination(selector: String, limit: Int = Conf.fetchLimit): RDD[Page] = self.flatMap {
    page => {
      val results = ArrayBuffer[Page](page)

      var currentPage = page
      var i = 0
      while (currentPage.attrExist(selector,"abs:href") && i< limit) {
        i = i+1
        val nextUrl = currentPage.href1(selector) //not null because already validated
        currentPage = PageBuilder.resolve(Wget(nextUrl)).toList(0)
        results.+=(currentPage.copy(context = page.context))
      }

      results
    }
  }

  //TODO: this will automatically detect patterns from urls of pages and advance in larger batch
  //  def smartJoinByPagination(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href")

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
  //whether we split the old pages, crawl the disambiguation part, >!<+ (lookup only the new plan from old pages) to new pages, and merge with the old pages
  //or we crawl the disambiguation part, merge with old ActionPlans and >!<+ (lookup all plans from old pages) to get the new pages?
  //I prefer the first one, potentially produce smaller footage.
  //TODO: there is no repartitioning in the process, may cause unbalanced execution
  def replaceIf(selector: String, limit: Int = Conf.fetchLimit, attr: String = "abs:href")(condition: String = selector): RDD[Page] = {
    val groupedPageRDD = self.map{ page => (page.elementExist(condition), page) }
    val falsePageRDD = groupedPageRDD.filter(_._1 == false).map(_._2)
    val truePageRDD = groupedPageRDD.filter(_._1 == true).map(_._2)
    val newPageRDD = truePageRDD.join(selector, limit, attr)
    newPageRDD.union(falsePageRDD)
  }

  def wgetReplaceIf(selector: String, limit: Int = Conf.fetchLimit, attr: String = "abs:href")(condition: String = selector): RDD[Page] = {
    val groupedPageRDD = self.map{ page => (page.elementExist(condition), page) }
    val falsePageRDD = groupedPageRDD.filter(_._1 == false).map(_._2)
    val truePageRDD = groupedPageRDD.filter(_._1 == true).map(_._2)
    val newPageRDD = truePageRDD.wgetJoin(selector, limit, attr)
    newPageRDD.union(falsePageRDD)
  }
  //----------------------------------------------
  //save should move to Actions
}