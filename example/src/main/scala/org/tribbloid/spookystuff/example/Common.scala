package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.actions.{Submit, TextInput, Visit}
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.expressions.Expression

/**
 * Created by peng on 22/07/15.
 */
object Common {

  def googleSearch(col: Expression[Any]) = Visit("http://www.google.com/") +>
    TextInput("input[name=\"q\"]",col) +>
    Submit("input[name=\"btnG\"]")
}
