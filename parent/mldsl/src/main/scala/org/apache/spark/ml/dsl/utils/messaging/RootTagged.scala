package org.apache.spark.ml.dsl.utils.messaging

trait RootTagged {

  def rootTag: String = Relay.RootTagOf(this).fallback
}
