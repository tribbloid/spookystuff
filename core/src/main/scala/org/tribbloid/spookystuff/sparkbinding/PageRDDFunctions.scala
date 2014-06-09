package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.tribbloid.spookystuff.entity.Page

//this is an implicit RDD view of (url,Page)
//all intermediate results after an action can be persisted to memory or disk, thus reusable.
class PageRDDFunctions(val self: RDD[(String, Page)]) {

//  private def visit()
//
//  private def visitNew()
//
//  private def visitOld(oldPageRDD: PageRDDOps)

//----------------------------------------------transformations

//  def linkFirst(selector: String): PageRDDFunctions = {}
//
//  def linkAll(selector: String): PageRDDFunctions = {}
//
////  def recursiveLinkFirst(selector: String)(max: Int = 100): PageRDDOps = {}
//
//  def interact(actions: Action*): PageRDDFunctions = {} //everything in one session
//
////  def interact(f: WebDriver => Iterable[Page]): PageRDDOps = {}
//
////  def recursiveInteract(actions: Action*)(max: Int = 100): PageRDDOps = {}
//
//  def slice(selector: String): PageRDDFunctions = {}
//
////----------------------------------------------actions???
//
//  def extractFirst(selector: String): RDD[(String, Map[String, Serializable])] = {}
//
//  def extractAll(selector: String)(): RDD[(String, Map[String, Serializable])] = {}

//----------------------------------------------

//  def saveAll(path: String)
}