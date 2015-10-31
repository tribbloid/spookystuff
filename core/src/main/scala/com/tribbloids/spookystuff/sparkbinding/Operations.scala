package com.tribbloids.spookystuff.sparkbinding

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.expressions._
import com.tribbloids.spookystuff.pages.{Page, Unstructured}
import com.tribbloids.spookystuff.row.Key
import com.tribbloids.spookystuff.{Const, QueryException}

/**
 * Created by peng on 30/10/15.
 */
abstract class Operation {

  final def apply(v1: PageRowRDD): PageRowRDD = {

    import com.tribbloids.spookystuff.utils.Views._

    v1.sparkContext.withJob(this.toString){
      _apply(v1)
    }
  }

  def _apply(v1: PageRowRDD): PageRowRDD
}

object Operations {

  import com.tribbloids.spookystuff.dsl._

  abstract class SelectLike(force: Boolean)(
    exprs: Expression[Any]*
    ) extends Operation {


    override def _apply(rdd: PageRowRDD): PageRowRDD = {

      val exprs = this.exprs

      val newKeys: Seq[Key] = exprs.flatMap {
        expr =>
          if (expr.name == null) None
          else {
            val key = Key(expr.name)
            if(!force && rdd.keys.contains(key) && !expr.isInstanceOf[ForceExpression[_]]) //can't insert the same key twice
              throw new QueryException(s"Key ${key.name} already exist")
            Some(key)
          }
      }

      val result = rdd.copy(
        selfRDD = rdd.flatMap(_.select(exprs: _*)),
        keys = rdd.keys ++ newKeys
      )
      result
    }
  }

  case class Select(
                     exprs: Expression[Any]*
                     ) extends SelectLike(false)(exprs:_*)

  case class Select_!(
                       exprs: Expression[Any]*
                       ) extends SelectLike(true)(exprs:_*)

  case class Remove(
                     keys: Symbol*
                     ) extends Operation {

    def _apply(rdd: PageRowRDD): PageRowRDD = {

      val keys = this.keys

      val names = keys.map(key => Key(key))
      rdd.copy(
        selfRDD = rdd.map(_.remove(names: _*)),
        keys = rdd.keys -- names
      )
    }
  }

  case class Flatten(
                      expr: Expression[Any],
                      ordinalKey: Symbol,
                      maxOrdinal: Int,
                      left: Boolean
                      ) extends Operation {

    def _apply(rdd: PageRowRDD): PageRowRDD = {
      val selected = rdd.select(expr)

      val flattened = selected.flatMap(_.flatten(expr.name, ordinalKey, maxOrdinal, left))
      selected.copy(
        selfRDD = flattened,
        keys = selected.keys ++ Option(Key.sortKey(ordinalKey))
      )
    }
  }

  case class FlatSelect(
                         expr: Expression[Iterable[Unstructured]],
                         ordinalKey: Symbol,
                         maxOrdinal: Int,
                         left: Boolean
                         )(exprs: Expression[Any]*) extends Operation {

    def _apply(rdd: PageRowRDD): PageRowRDD = {
      rdd
        .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), ordinalKey, maxOrdinal, left)
        .select(exprs: _*)
    }
  }

  case class Fetch(
                    traces: Set[Trace],
                    joinType: JoinType,
                    flattenPagesPattern: String,
                    flattenPagesOrdinalKey: Symbol,
                    numPartitions: Int,
                    optimizer: QueryOptimizer
                    ) extends Operation {

    def _apply(rdd: PageRowRDD): PageRowRDD = {

      val _traces = traces.autoSnapshot

      rdd.spooky.broadcast()

      val result = optimizer match {
        case Narrow =>
          rdd._narrowFetch(_traces, joinType, numPartitions)
        case Wide =>
          rdd._wideFetch(_traces, joinType, numPartitions, useWebCache = false)()
            .discardDataRowsInWebCacheRDD //optional
        case Wide_RDDWebCache =>
          rdd._wideFetch(_traces, joinType, numPartitions, useWebCache = true)()
            .discardDataRowsInWebCacheRDD
        case _ => throw new NotImplementedError(s"${optimizer.getClass.getSimpleName} optimizer is not supported")
      }

      if (flattenPagesPattern != null) result.flattenPages(flattenPagesPattern, flattenPagesOrdinalKey)
      else result
    }
  }

  case class Join(
                   expr: Expression[Any],
                   ordinalKey: Symbol,
                   maxOrdinal: Int,
                   distinct: Boolean
                   )(
                   traces: Set[Trace],
                   joinType: JoinType,
                   numPartitions: Int,
                   flattenPagesPattern: String,
                   flattenPagesOrdinalKey: Symbol,
                   optimizer: QueryOptimizer
                   )(
                   select: Expression[Any]*
                   ) extends Operation {

    def _apply(rdd: PageRowRDD): PageRowRDD = {

      var flat = rdd.clearTempData
        .flattenTemp(expr defaultAs Symbol(Const.defaultJoinKey), ordinalKey, maxOrdinal, left = true)

      if (distinct) flat = flat.distinctBy(expr)

      flat.fetch(traces, joinType, flattenPagesPattern, flattenPagesOrdinalKey, numPartitions, optimizer)
        .select(select: _*)
    }
  }

  case class Explore(
                      expr: Expression[Any],
                      depthKey: Symbol,
                      maxDepth: Int,
                      ordinalKey: Symbol,
                      maxOrdinal: Int,
                      checkpointInterval: Int
                      )(
                      traces: Set[Trace],
                      numPartitions: Int,
                      flattenPagesPattern: String,
                      flattenPagesOrdinalKey: Symbol,
                      optimizer: QueryOptimizer
                      )(
                      select: Expression[Any]*
                      ) extends Operation {

    def _apply(rdd: PageRowRDD): PageRowRDD = {

      val _traces = traces.autoSnapshot

      rdd.spooky.broadcast()

      val cleared = rdd.clearTempData

      val result = optimizer match {
        case Narrow =>
          cleared._narrowExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
        case Wide =>
          cleared._wideExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval, useWebCache = false)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
        case Wide_RDDWebCache =>
          cleared._wideExplore(expr, depthKey, maxDepth, ordinalKey, maxOrdinal, checkpointInterval, useWebCache = true)(_traces, numPartitions, flattenPagesPattern, flattenPagesOrdinalKey)(select: _*)
        case _ => throw new NotImplementedError(s"${optimizer.getClass.getSimpleName} optimizer is not supported")
      }

      result
    }
  }

  case class SavePages(
                        path: Expression[Any],
                        ext: Expression[Any],
                        pageExpr: Expression[Page],
                        overwrite: Boolean
                        ) extends Operation {

    def _apply(rdd: PageRowRDD): PageRowRDD = {

      val effectiveExt = if (ext != null) ext
      else pageExpr.defaultExt

      rdd.spooky.broadcast()
      val spooky = rdd.spooky

      rdd.foreach {
        pageRow =>
          var pathStr: Option[String] = path(pageRow).map(_.toString).map {
            str =>
              val splitted = str.split(":")
              if (splitted.size <= 2) str
              else splitted.head + ":" + splitted.slice(1, Int.MaxValue).mkString("%3A") //colon in file paths are reserved for protocol definition
          }

          val extOption = effectiveExt(pageRow)
          if (extOption.nonEmpty) pathStr = pathStr.map(_ + "." + extOption.get.toString)

          pathStr.foreach {
            str =>
              val page = pageExpr(pageRow)

              spooky.metrics.pagesSaved += 1

              page.foreach(_.save(Seq(str), overwrite)(spooky))
          }
      }
      rdd
    }
  }
}
