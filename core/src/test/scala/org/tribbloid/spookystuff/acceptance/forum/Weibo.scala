//package org.tribbloid.spookystuff.acceptance.forum
//
//import org.tribbloid.spookystuff.SpookyContext._
//import org.tribbloid.spookystuff.acceptance.SpookyTestCore
//import org.tribbloid.spookystuff.entity._
//
///**
//* Created by peng on 8/28/14.
//*/
//object Weibo extends SpookyTestCore {
//
//  def doMain() = {
//
//    (sc.parallelize(Seq("Battle of the five armies")) +>
//      Visit("http://www.weibo.com/login.php") +>
//      TextInput("div.username input.W_input","peng@anchorbot.com") +>
//      TextInput("div.password input.W_input","A9e7k1") +>
//      Click("a.W_btn_g span",40) +>
//      Delay(20)
//      !==)
//      .saveAs(dir="file:///home/peng/spooky/weibo").collect()
//  }
//}