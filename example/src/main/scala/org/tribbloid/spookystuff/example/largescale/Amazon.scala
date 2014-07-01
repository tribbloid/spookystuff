//package org.tribbloid.spookystuff.example
//
//import org.tribbloid.spookystuff.SpookyContext._
//import org.tribbloid.spookystuff.entity._
//
///**
//* A more complex linkedIn job that finds name and printout skills of all Sanjay Gupta in your local area
//*/
////remember infix operator cannot be written in new line
//object Amazon extends SparkSubmittable {
//
//  def doMain() {
//
//    (sc.parallelize(Seq("Lord of the rings")) +>
//      Visit("http://www.amazon.ca/") +>
//      TextInput("input#twotabsearchtextbox", "#{_}") +>
//      Submit("input.nav-submit-input") !)
//      .map{ page => (
//      page.text1("div#result_0 li.newp span.red"),
//      page.text1("p.title")
//      )
//    }.collect().foreach(println(_))
//  }
//}
