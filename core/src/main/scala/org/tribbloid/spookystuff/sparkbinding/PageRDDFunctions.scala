package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.Page
import scala.collection.mutable

//this is an implicit RDD view of Page
//all intermediate results after a transformation can be persisted to memory or disk, thus reusable.
//mimic sql keywords
class PageRDDFunctions(val self: RDD[Page]) {

  def as(alias: String): RDD[Page] = self.map{ _.as(alias) }

  def from(alias: String): RDD[Page] = self.filter{ _.alias == alias }

  def select(fMap: (String, Page => Serializable)*): RDD[Map[String, Serializable]] = self.map {

    page => {
      val result: mutable.Map[String, Serializable] = mutable.HashMap.empty[String, Serializable]

      fMap.foreach{
        fEntity => result.put(fEntity._1, fEntity._2(page))
      }

      result.toMap
    }
  }

  def where(f: Page => Boolean) = self.filter(f)

  //TODO: finish ActionRDDFunctions first!
//  def linkFirst(): RDD[Page] = self.map{
//    page => {
//
//    }
//  }.distinct
//  .map{
//
//  }
//
//  def linkAll(): RDD[Page] = self.flatMap{
//
//  }.distinct
//  .map{
//
//  }

//  def slice(): RDD[Page]

  //----------------------------------------------
  //
  //  def save(fileName: String, usePattern: Boolean = false) {
  //
  //  }
}