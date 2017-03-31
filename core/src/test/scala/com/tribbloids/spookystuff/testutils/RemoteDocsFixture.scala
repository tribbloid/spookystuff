package com.tribbloids.spookystuff.testutils

/**
  * Created by peng on 17/05/16.
  */
trait RemoteDocsFixture extends FunSuitex {

  def HTML_URL = "http://tribbloid.github.io/spookystuff/test/Wikipedia.html"
  def JSON_URL = "http://tribbloid.github.io/spookystuff/test/tribbloid.json"
  //TODO: add this after fetch can semi-auto-detect content-type
  //  def jsonUrlIncorrectContentType = "https://raw.githubusercontent.com/tribbloid/spookystuff/master/core/src/test/resources/site/tribbloid.json"
  def PNG_URL = "http://tribbloid.github.io/spookystuff/test/logo11w.png"
  def PDF_URL = "http://tribbloid.github.io/spookystuff/test/Test.pdf"
  def XML_URL = "http://tribbloid.github.io/spookystuff/test/pom.xml"
  def CSV_URL = "http://tribbloid.github.io/spookystuff/test/table.csv"

  def HTTP_IP_URL = "http://api.ipify.org/"
  def HTTPS_IP_URL = "https://api.ipify.org/"
  def USERAGENT_URL = "http://www.whatsmyuseragent.com/"
}