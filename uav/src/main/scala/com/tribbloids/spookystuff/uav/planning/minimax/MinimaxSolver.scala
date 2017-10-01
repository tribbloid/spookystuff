package com.tribbloids.spookystuff.uav.planning.minimax

import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import com.tribbloids.spookystuff.uav.planning.PathPlanningSolver

trait MinimaxSolver extends PathPlanningSolver[GenPartitioners.MinimaxCost]
