package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.Action

case class IndexedAction(
                          action: Action,
                          i: (Int, Long)
                        )

case class IndexedPair(
                        start: IndexedAction,
                        end: IndexedAction
                      )