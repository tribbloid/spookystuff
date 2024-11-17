package com.tribbloids.spookystuff.commons.lifespan

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable

trait LocalCleanable extends Cleanable with NOTSerializable
