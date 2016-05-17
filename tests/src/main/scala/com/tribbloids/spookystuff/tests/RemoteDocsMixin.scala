package com.tribbloids.spookystuff.tests

/**
  * Created by peng on 17/05/16.
  */
trait RemoteDocsMixin extends TestMixin {

  def HTML_URL = "http://tribbloid.github.io/spookystuff/test/Wikipedia.html"
  def JSON_URL = "http://tribbloid.github.io/spookystuff/test/tribbloid.json"
  //TODO: add this after fetch can semi-auto-detect content-type
  //  def jsonUrlIncorrectContentType = "https://raw.githubusercontent.com/tribbloid/spookystuff/master/core/src/test/resources/site/tribbloid.json"
  def PNG_URL = "https://www.google.ca/images/srpr/logo11w.png"
  def PDF_URL = "http://stlab.adobe.com/wiki/images/d/d3/Test.pdf"
  def XML_URL = "http://tribbloid.github.io/spookystuff/test/pom.xml"
  def CSV_URL = "http://tribbloid.github.io/spookystuff/test/table.csv"
}