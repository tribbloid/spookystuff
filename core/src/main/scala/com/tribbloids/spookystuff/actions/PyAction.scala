package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.session.python.MessageInstanceRef

/**
  * Created by peng on 01/11/16.
  */
trait PyAction extends Action with MessageInstanceRef {

  /**
    * must have exactly the same class/function under the same package imported in python that takes 2 JSON strings
    * 1 for action, 1 for session
    */
  //TODO: delegate to something else
  final override def exe(session: Session): Seq[Fetched] = {
    withLazyDrivers(session){
      try {
        doExe(session)
      }
      finally {
        this.Py(session).clean()
      }
    }
  }
}
