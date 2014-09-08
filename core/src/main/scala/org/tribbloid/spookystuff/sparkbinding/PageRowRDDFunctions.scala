package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.operator.{JoinType, LeftOuter, Merge, Replace}
import org.tribbloid.spookystuff.{Const, SpookyContext}

/**
 * Created by peng on 8/29/14.
 */
//TODO: where is it serialized?
class PageRowRDDFunctions(@transient val self: RDD[PageRow])(@transient val spooky: SpookyContext) extends Serializable{

  /**
   * append an action
   * @param action any object that inherits org.tribbloid.spookystuff.entity.ClientAction
   * @return new RDD[ActionPlan]
   */
  def +>(action: ClientAction): RDD[PageRow] = self.map {
    _ +> action
  }

  /**
   * append a series of actions
   * equivalent to ... +> action1 +> action2 +>...actionN, where action{1...N} are elements of actions
   * @param actions = Seq(action1, action2,...)
   * @return new RDD[ActionPlan]
   */
  def +>(actions: Seq[ClientAction]): RDD[PageRow] = self.map {
    _ +> actions
  }

  def +>(pr: PageRow): RDD[PageRow] = self.map {
    _ +> pr
  }

  /**
   * append a set of actions or ActionPlans to be executed in parallel
   * each old ActionPlan yields N new ActionPlans, where N is the size of the new set
   * results in a cartesian product of old and new set.
   * @param actions a set of Actions/Sequences to be appended and executed in parallel
   *                can be Seq[ClientAction], Seq[ Seq[ClientAction] ] or Seq[ActionPlan], or any of their combinations.
   *                CANNOT be RDD[ActionPlan], but this may become an option in the next release
   * @return new RDD[ActionPlan], of which size = [size of old RDD] * [size of actions]
   */
  def +*>(actions: Seq[_]): RDD[PageRow] = self.flatMap {
    _ +*> actions
  }

  def asMapRDD(): RDD[Map[String,Any]] = self.map {
    _.cells
  }

  def asJsonRDD(): RDD[String] = self.map {
    _.asJson()
  }

  def asSchemaRDD(): SchemaRDD = {

    self.persist() //for some unknown reason SQLContext.jsonRDD uses the parameter RDD twice, this has to be fixed by somebody else

    val result = this.spooky.sql.jsonRDD(this.asJsonRDD())

    result
  }

  def asCsvRDD(separator: String = ","): RDD[String] = this.asSchemaRDD().map {
    _.mkString(separator)
  }

  def asTsvRDD(): RDD[String] = this.asCsvRDD("\t")
  
  /**
   * parallel execution in browser(s) to yield a set of web pages
   * each ActionPlan may yield several pages in a row, depending on the number of Export(s) in it
   * @return RDD[Page] as results of execution
   */
  def !=!(
           joinType: JoinType = Const.defaultJoinType,
           flatten: Boolean = true,
           indexKey: String = null
           ): RDD[PageRow] = {

    val spookyBroad = self.context.broadcast(this.spooky)

    implicit def spookyImplicit: SpookyContext = spookyBroad.value

    self.flatMap {
      _.!=!(joinType,flatten,indexKey)
    }
  }

  //  def !>><<(
  //             joinType: JoinType = Const.defaultJoinType,
  //             flatten: Boolean = true
  //             ): RDD[PageRow] = {
  //    val spookyBroad = self.context.broadcast(this.spooky)
  //
  //    import org.apache.spark.SparkContext._
  //nodule
  //    self.persist()
  //
  //    val squashedRDD = self.map {
  //      _.actions
  //    }.distinct()
  //
  //    val exeRDD = squashedRDD.map(actions => actions -> PageBuilder.resolve(actions: _*) )
  //
  //    self.map
  //
  //  }

  //TODO: this definitely need some logging to let us know how many actual resolves.
  //TODO: actionless row needs to be treated differently, current implementation skewed bad
  /**
   * smart execution: group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
   * reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
   * recommended for most cases, mandatory for RDD[ActionPlan] with high duplicate factor, only use !() if you are sure that duplicate doesn't exist.
   * @return RDD[Page] as results of execution
   */
  def !><(
           joinType: JoinType = Const.defaultJoinType,
           flatten: Boolean = true,
           indexKey: String = null
           ): RDD[PageRow] = {
    val spookyBroad = self.context.broadcast(this.spooky)

    implicit def spookyImplicit: SpookyContext = spookyBroad.value

    import org.apache.spark.SparkContext._

    val squashedRDD = self.map {
      selfRow => {

        (selfRow.actions, selfRow)
      }
    }.groupByKey()

    squashedRDD.flatMap {
      tuple => {
        val newPages = PageBuilder.resolve(tuple._1: _*)

        var newPageRows = joinType match {
          case Replace if newPages.isEmpty =>
            tuple._2.map( oldPageRow => PageRow(cells = oldPageRow.cells, pages = oldPageRow.pages) )
          case Merge =>
            tuple._2.map( oldPageRow => PageRow(cells = oldPageRow.cells, pages = oldPageRow.pages ++ newPages) )
          case _ =>
            tuple._2.map( oldPageRow => PageRow(cells = oldPageRow.cells, pages = newPages) )
        }

        if (flatten) newPageRows = newPageRows.flatMap(_.flatten(joinType == LeftOuter, indexKey))

        newPageRows
      }
    }
  }

  /**
   * extract parts of each Page and insert into their respective context
   * if a key already exist in old context it will be replaced with the new one.
   * @param keyAndF a key-function map, each element is used to generate a key-value map using the Page itself
   * @return new RDD[Page]
   */
  def select(keyAndF: (String, Page => Any)*): RDD[PageRow] = self.map {
    _.select(keyAndF: _*)
  }

  def unselect(keys: String*): RDD[PageRow] = self.map {
    _.unselect(keys: _*)
  }

  /**
   * save each page to a designated directory
   * this is a narrow transformation, use it to save overhead for scheduling
   * support many file systems including but not limited to HDFS, S3 and local HDD
   * @param root (file/s3/s3n/hdfs)://path
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return the same RDD[Page] with file paths carried as metadata
   */
  def saveAs(
              root: String = this.spooky.saveRoot, //support context interpolation
              select: Page => String = this.spooky.saveSelect,
              overwrite: Boolean = false
              ): RDD[PageRow] = {
    val hconfBroad = self.context.broadcast(this.spooky.hConf)

    self.map {

      pageRow => {

        val rootInterpolated = ClientAction.interpolate(root, pageRow.cells)

        val newPages = pageRow.pages.map(page => page.save(spooky.pagePath(page, rootInterpolated, select), overwrite = overwrite)(hconfBroad.value))

        pageRow.copy(pages = newPages)
      }
    }
  }

  /**
   * same as saveAs
   * but this is an action that will be executed immediately
   * @param root (file/s3/s3n/hdfs)://path
   * @param overwrite if a file with the same name already exist:
   *                  true: overwrite it
   *                  false: append an unique suffix to the new file name
   * @return an array of file paths
   */
  def dump(
            root: String = this.spooky.saveRoot, //support context interpolation
            select: Page => String = this.spooky.saveSelect,
            overwrite: Boolean = false
            ): Array[String] = {

    this.saveAs(root, select, overwrite).flatMap{
      _.pages.map{
        _.filePath
      }
    }.collect()
  }

  def +%>(
           actionAndF: (ClientAction, Page => _)
           ): RDD[PageRow] = self.map( _.+%>(actionAndF) )

  def +*%>(
            actionAndF: (ClientAction, Page => Array[_])
            )(
            distinct: Boolean = true,
            limit: Int = Const.fetchLimit, //applied after distinct
            indexKey: String = null
            ): RDD[PageRow] = self.flatMap(_.+*%>(actionAndF)(distinct,limit,indexKey))

  def dropActions(): RDD[PageRow] = {

    self.map {
      _.copy(actions = Seq(), dead = false)
    }
  }

  //  private def join(
  //                    action: ClientAction,
  //                    f: Page => Array[_]
  //                    )(
  //                    distinct: Boolean = true,
  //                    limit: Int = Const.fetchLimit, //applied after distinct
  //                    indexKey: String = null
  //                    joinType: JoinType = Const.defaultJoinType,
  //                    flatten: Boolean = true
  //                    ): RDD[PageRow] = {
  //
  //    import spooky._
  //
  //    this.cleanActions.+%>(action, f).!><
  //  }

  /**
   * generate a set of ActionPlans that crawls from current Pages by visiting their links
   * all context of Pages will be persisted to the resulted ActionPlans
   * pages that doesn't contain the link will be ignored
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[ActionPlan]
   */
  def visit(
             selector: String,
             attr: String = "abs:href"
             )(
             distinct: Boolean = true,
             limit: Int = Const.fetchLimit, //applied after distinct
             indexKey: String = null
             ): RDD[PageRow] =
    this.+*%>(Visit("#{~}") -> (_.attr(selector, attr))
    )(distinct, limit, indexKey)

  def wget(
                    selector: String,
                    attr: String = "abs:href"
                    )(
                    distinct: Boolean = true,
                    limit: Int = Const.fetchLimit, //applied after distinct
                    indexKey: String = null
                    ): RDD[PageRow] =
    this.+*%>(Wget("#{~}") -> (_.attr(selector, attr))
    )(distinct, limit, indexKey)

  /**
   * results in a new set of Pages by crawling links on old pages
   * old pages that doesn't contain the link will be ignored
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @param attr attribute of the element that denotes the link tar  implicit def spookyImplicit: SpookyContext = spookyBroad.valueget, default to absolute href
   * @return RDD[Page]
   */
  def join(
            selector: String,
            attr :String = "abs:href"
            )(
            distinct: Boolean = true,
            limit: Int = Const.fetchLimit, //applied after distinct
            indexKey: String = null,
            joinType: JoinType = Const.defaultJoinType,
            flatten: Boolean = true
            ): RDD[PageRow] ={

    val spookyBroad = self.context.broadcast(this.spooky)

    implicit def spookyImplicit: SpookyContext = spookyBroad.value

    import spooky._

    this.dropActions().visit(selector, attr)(distinct, limit, indexKey).!><(joinType, flatten)
  }

  /**
   * same as join, but avoid launching a browser by using direct http GET (wget) to download new pages
   * much faster and less stressful to both crawling and target server(s)
   * @param selector css selector of page elements with a crawlable link
   * @param limit only the first n links will be used, default to Const.fetchLimit
   * @param attr attribute of the element that denotes the link target, default to absolute href
   * @return RDD[Page]
   */
  def wgetJoin(
                selector: String,
                attr :String = "abs:href"
                )(
                distinct: Boolean = true,
                limit: Int = Const.fetchLimit, //applied after distinct
                indexKey: String = null,
                joinType: JoinType = Const.defaultJoinType,
                flatten: Boolean = true
                ): RDD[PageRow] ={

    val spookyBroad = self.context.broadcast(this.spooky)

    implicit def spookyImplicit: SpookyContext = spookyBroad.value

    import spooky._

    this.dropActions().wget(selector, attr)(distinct, limit, indexKey).!><(joinType, flatten)
  }

  /**
   * break each page into 'shards', used to extract structured data from tables
   * @param selector denotes enclosing elements of each shards
   * @param limit only the first n elements will be used, default to Const.fetchLimit
   * @return RDD[Page], each page will generate several shards
   */
  def sliceJoin(
                 selector: String,
                 expand :Int = 0
                 )(
                 limit: Int = Const.fetchLimit, //applied after distinct
                 indexKey: String = null,
                 joinType: JoinType = Const.defaultJoinType,
                 flatten: Boolean = true
                 ): RDD[PageRow] ={

    self.flatMap {

      _.dropActions().slice(selector, expand)(limit, indexKey, joinType, flatten)
    }
  }

  /**
   * insert many pages for each old page by recursively visiting "next page" link
   * @param selector selector of the "next page" element
   * @param limit depth of recursion
   * @return RDD[Page], contains both old and new pages
   */
  def paginate(
              selector: String,
              attr :String = "abs:href",
              wget: Boolean = true
              )(
              limit: Int = Const.fetchLimit,
              indexKey: String = null,
              flatten: Boolean = true
              ): RDD[PageRow] = {

    val spookyBroad = self.context.broadcast(this.spooky)

    implicit def spookyImplicit: SpookyContext = spookyBroad.value

    self.flatMap {
      _.paginate(selector, attr, wget)(limit, indexKey, flatten)
    }
  }

}
