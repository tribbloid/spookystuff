//package com.tribbloids.spookystuff.execution
//
//import com.tribbloids.spookystuff.row._
//import org.apache.spark.rdd.RDD
//
///**
//  * discard pages
//  */
//case class AggregatePlan(
//    override val child: ExecutionPlan,
//    exprs: Seq[(FetchedRow => Any)]
//) extends UnaryPlan(child) {
//
//  override def doExecute(): SquashedRDD = {
//    val grouped: RDD[(Seq[Any], Vector[DataRow.WithScope])] = super.fetchedRDD
//      .keyBy { row =>
//        exprs.map(expr => expr.apply(row))
//      }
//      .mapValues(v => Vector(v.dataRowWithScope))
//
//    grouped
//      .map(v => SquashedRow.ofData(v._2: _*))
//  }
//}
