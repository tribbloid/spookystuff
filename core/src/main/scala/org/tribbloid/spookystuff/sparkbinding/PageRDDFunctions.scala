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

/**
 * An implicit wrapper class for RDD[Page], representing a set of pages each containing a datum being scraped.
 * Constructed by executing an RDD[ActionPlan]
 */
class PageRDDFunctions(val self: RDD[Page]) {

  /**
   * rename all Pages
   * @param alias new alias
   * @return new RDD[Page]
   */
  def as(alias: String): RDD[Page] = self.map{ _.copy(alias = alias) }

  /**
   * remove all pages from the set except those with a specified alias
   * @param alias
   * @return new RDD[Page]
   */
  def from(alias: String): RDD[Page] = self.filter{ _.alias == alias }

  /**
   * remove all context from each Page
   * @return new RDD[Page]
   */
  def clearContext(): RDD[Page] = self.map{ _.copy(context = null) }

  //TODO: this is the most abominable interface so far, will gradually evolve to resemble Spark SQL's select
  /**
   * extract parts of each Page and insert into their respective context
   * if a key already exist in old context it will be replaced with the new one.
   * @param keyAndF a key-function map, each element is used to generate a key-value map using the Page itself
   * @return new RDD[Page]
   */
  def selectInto(keyAndF: (String, Page => Serializable)*): RDD[Page] = self.map {

    page => {
      val map = page.extractPropertiesAsMap(keyAndF: _*)

      //always replace old key-value pairs with new ones, old ones are flushed out
      val newContext = new util.HashMap[String,Serializable](page.context)
      newContext.putAll(map)

      page.copy(context = newContext)
    }
  }

  /**
   * same as select, but will remove old context
   * used to reshape context structure
   * @param keyAndF
   * @return new RDD[Page]
   */
  def select(keyAndF: (String, Page => Serializable)*): RDD[Page] = self.map {

    page => {
      val map = page.extractPropertiesAsMap(keyAndF: _*)
      page.copy(context = map)
    }
  }

  /**
   * save each page to a designated directory
   * this is a narrow transformation, use it to save overhead for scheduling
   * support many file systems including but not limited to HDFS, S3 and local HDD
   * @param fileName string pattern used to generate different names for different pages
   * @param dir (file/s3/s3n/hdfs)://path
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return the same RDD[Page] with file paths carried as metadata
   */
  def saveAs(fileName: String = "#{resolved-url}", dir: String = Conf.savePagePath, overwrite: Boolean = false): RDD[Page] = self.map {
    val hConfWrapper = self.context.broadcast(new SerializableWritable(self.context.hadoopConfiguration))

    page => {
      val name = page.save(fileName, dir, overwrite)(hConfWrapper.value.value)

      page.copy(savePath = name)
    }
  }

  /**
   * same as saveAs
   * but this is an action that will be executed immediately
   * @param fileName string pattern used to generate different names for different pages
   * @param dir (file/s3/s3n/hdfs)://path
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return an array of file paths
   */
  def dump(fileName: String = "#{resolved-url}", dir: String = Conf.savePagePath, overwrite: Boolean = false): Array[String] = {
    val hConfWrapper = self.context.broadcast(new SerializableWritable(self.context.hadoopConfiguration))

    self.map(page => page.save(fileName, dir, overwrite)(hConfWrapper.value.value)).collect()
  }

  /**
   * generate a set of ActionPlans that crawls from current Pages by visiting their links
   * all context of Pages will be persisted to the resulted ActionPlans
   * pages that doesn't contain the link will be ignored
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[ActionPlan]
   */
  def visit(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[ActionPlan] = self.flatMap{
    page => {

      val context = page.context
      page.attr(selector,attr,limit).map {
        str => new ActionPlan(context, Visit(str))
      }
    }
  }

  /**
   * same as visit, but pages that doesn't contain the link will yield an EmptyActionPlan
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[ActionPlan]
   */
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

  /**
   * results in a new set of Pages by crawling links on old pages
   * old pages that doesn't contain the link will be ignored
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[Page]
   */
  def join(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.visit(selector, limit, attr) !><

  /**
   * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[Page]
   */
  def wgetJoin(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.wget(selector, limit, attr) !><

  /**
   * same as join, but old pages that doesn't contain the link will yield an EmptyPage
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[Page]
   */
  def leftJoin(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.leftVisit(selector, limit, attr) !><

  /**
   * same as wgetJoin, but old pages that doesn't contain the link will yield an EmptyPage
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[Page]
   */
  def wgetLeftJoin(selector: String, limit: Int = Conf.fetchLimit, attr :String = "abs:href"): RDD[Page] =
    this.leftWget(selector, limit, attr) !><

  /**
   * break each page into 'shards', used to extract structured data from tables
   * @param selector denotes enclosing elements of each shards
   * @param as alias of resulted shards
   * @param limit only the first n elements will be used, default to Conf.fetchLimit
   * @return RDD[Page], each page will generate several shards
   */
  def joinBySlice(selector: String, as: String = null, limit: Int = Conf.fetchLimit): RDD[Page] =
    self.flatMap(_.slice(selector, as, limit))

  /**
   * same as joinBySlice, but old pages that doesn't contain the element will yield an EmptyPage
   * @param selector denotes enclosing elements of each shards
   * @param as alias of resulted shards
   * @param limit only the first n elements will be used, default to Conf.fetchLimit
   * @return RDD[Page], each page will generate several shards
   */
  def leftJoinBySlice(selector: String, as: String = null, limit: Int = Conf.fetchLimit): RDD[Page] =
    self.flatMap {
      page => {

        val results = page.slice(selector, as, limit)
        if (results.size==0) Seq(PageBuilder.emptyPage.copy(context = page.context))
        else results
      }
    }

  /**
   * insert many pages for each old page by recursively visiting "next page" link
   * link attribute is always "abs:href"
   * @param selector selector of the "next page" element
   * @param limit depth of recursion
   * @return RDD[Page], contains both old and new pages
   */
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

  /**
   * same as insertPagination, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param selector selector of the "next page" element
   * @param limit depth of recursion
   * @return RDD[Page], contains both old and new pages
   */
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
  /**
   * same as join, but only crawls pages that meet the condition, keep other pages in the result
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @param condition css selector, if elements denoted by this selector doesn't exist on the page, the page will not be crawled, instead it will be retained in the result
   * @return
   */
  def joinOrKeep(selector: String, limit: Int = Conf.fetchLimit, attr: String = "abs:href")(condition: String = selector): RDD[Page] = {
    val groupedPageRDD = self.map{ page => (page.elementExist(condition), page) }
    val falsePageRDD = groupedPageRDD.filter(_._1 == false).map(_._2)
    val truePageRDD = groupedPageRDD.filter(_._1 == true).map(_._2)
    val newPageRDD = truePageRDD.join(selector, limit, attr)
    newPageRDD.union(falsePageRDD)
  }

  /**
   * same as wgetJoin, but only crawls pages that meet the condition, keep other pages in the result
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Conf.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @param condition css selector, if elements denoted by this selector doesn't exist on the page, the page will not be crawled, instead it will be retained in the result
   * @return
   */
  def wgetJoinOrKeep(selector: String, limit: Int = Conf.fetchLimit, attr: String = "abs:href")(condition: String = selector): RDD[Page] = {
    val groupedPageRDD = self.map{ page => (page.elementExist(condition), page) }
    val falsePageRDD = groupedPageRDD.filter(_._1 == false).map(_._2)
    val truePageRDD = groupedPageRDD.filter(_._1 == true).map(_._2)
    val newPageRDD = truePageRDD.wgetJoin(selector, limit, attr)
    newPageRDD.union(falsePageRDD)
  }
}