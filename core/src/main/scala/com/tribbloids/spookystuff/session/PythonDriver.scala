package com.tribbloids.spookystuff.session

/**
  * Created by peng on 01/08/16.
  */
class PythonDriver(
                    binPath: String
                  ) extends PythonProcess(binPath) with Clean {

  //  def
  override def clean(): Unit = {
    this.close()
  }
}
